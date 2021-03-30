package coordinator

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/angopher/chronus/coordinator/internal"
	"github.com/angopher/chronus/coordinator/request"
	errs "github.com/go-errors/errors"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb/services/meta"
)

// Service and remote executor are designed for "forwarded" requests
//	between shards/nodes. For better performance we separate the design
//	into several phases:
//	1. Introduce connection pooling avoiding create/destroy connections frequently.
//	2. Introduce multiplexing to reduce the cost of connections further more.
//	But we should also notice that more than one connection are needed to avoid flow control of TCP.
// Now we implement connection pooling first and keep an versioning api which enables
//	data node declaring its running version.

// MaxMessageSize defines how large a message can be before we reject it
const MaxMessageSize = 1024 * 1024 * 1024 // 1GB

// MuxHeader is the header byte used in the TCP mux.
const MuxHeader = 2

type ServerResponse interface {
	SetCode(int)
	SetMessage(string)
	MarshalBinary() ([]byte, error)
}

// Service processes data received over raw TCP connections.
type Service struct {
	mu      sync.RWMutex
	Node    *influxdb.Node
	wg      sync.WaitGroup
	closing chan struct{}

	Listener net.Listener

	MetaClient interface {
		ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo)
	}

	TSDBStore   TSDBStore
	TaskManager *query.TaskManager

	Logger *zap.Logger
	stats  *internal.InternalServiceStatistics
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		closing: make(chan struct{}),
		//Logger:  log.New(os.Stderr, "[cluster] ", log.LstdFlags),
		stats:  &internal.InternalServiceStatistics{},
		Logger: zap.NewNop(),
	}
}

// Open opens the network listener and begins serving requests.
func (s *Service) Open() error {

	s.Logger.Info("Starting cluster service")
	// Begin serving connections.
	s.wg.Add(1)
	go s.serve()

	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "cluster"))
}

// serve accepts connections from the listener and handles them.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down.
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection.
		conn, err := s.Listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "connection closed") {
				s.Logger.Error("cluster service accept fail", zap.Error(err))
				return
			}
			s.Logger.Error("accept error", zap.Error(err))
			continue
		}

		// Delegate connection handling to a separate goroutine.
		s.wg.Add(1)
		go func() {
			defer func() {
				s.wg.Done()
				if err := recover(); err != nil {
					s.Logger.Error("recover from panic", zap.String("stack", errs.Wrap(err, 2).ErrorStack()))
				}
			}()

			s.handleConn(conn)
		}()
	}
}

// Close shuts down the listener and waits for all connections to finish.
func (s *Service) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Shut down all handlers.
	close(s.closing)
	s.wg.Wait()

	return nil
}

func (s *Service) handleRequest(typ byte, data []byte) (respType byte, resp encoding.BinaryMarshaler, err error) {
	// Delegate message processing by type.
	switch typ {
	case writeShardRequestMessage:
		err = s.processWriteShardRequest(data)
		respType = writeShardResponseMessage
		resp = &WriteShardResponse{}
	case executeStatementRequestMessage:
		err = s.processExecuteStatementRequest(data)
		respType = writeShardResponseMessage
		resp = &WriteShardResponse{}
	case fieldDimensionsRequestMessage:
		respType = fieldDimensionsResponseMessage
		resp, err = s.processFieldDimensionsRequest(data)
	case tagKeysRequestMessage:
		respType = tagKeysResponseMessage
		resp, err = s.processTagKeysRequest(data)
	case tagValuesRequestMessage:
		respType = tagValuesResponseMessage
		resp, err = s.processTagValuesRequest(data)
	case measurementNamesRequestMessage:
		respType = measurementNamesResponseMessage
		resp, err = s.processMeasurementNamesRequest(data)
	case seriesCardinalityRequestMessage:
		respType = seriesCardinalityResponseMessage
		resp, err = s.processSeriesCardinalityRequest(data)
	case deleteSeriesRequestMessage:
		respType = deleteSeriesResponseMessage
		resp, err = s.processDeleteSeriesRequest(data)
	case deleteDatabaseRequestMessage:
		respType = deleteDatabaseResponseMessage
		resp, err = s.processDeleteDatabaseRequest(data)
	case deleteMeasurementRequestMessage:
		respType = deleteMeasurementResponseMessage
		resp, err = s.processDeleteMeasurementRequest(data)
	case iteratorCostRequestMessage:
		respType = iteratorCostResponseMessage
		resp, err = s.processIteratorCostRequest(data)
	case mapTypeRequestMessage:
		respType = mapTypeResponseMessage
		resp, err = s.processMapTypeRequest(data)
	case executeTaskManagerRequestMessage:
		respType = executeTaskManagerResponseMessage
		resp, err = s.processTaskManagerRequest(data)
	case testRequestMessage:
		// do nothing
	default:
		s.Logger.Error("cluster service message type not found", zap.Uint8("type", typ))
	}
	if err != nil {
		s.Logger.Error("process request error", zap.Uint8("type", typ), zap.Error(err))
	}
	if resp == nil {
		return
	}
	if serverResp, ok := resp.(ServerResponse); ok {
		if err != nil {
			serverResp.SetCode(1)
			serverResp.SetMessage(err.Error())
		} else {
			serverResp.SetCode(0)
		}
	}
	return
}

func (s *Service) writeClusterResponse(conn io.Writer, respType byte, resp encoding.BinaryMarshaler) (err error) {
	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Error("error marshalling response", zap.Uint8("respType", respType), zap.Error(err))
		return
	}

	// Write to connection.
	if err = WriteTLV(conn, respType, buf); err != nil {
		s.Logger.Error("WriteTLV fail", zap.Error(err))
	}
	return
}

func canContinue(err error) bool {
	if err == io.EOF || err.Error() == "use of closed network connection" {
		return false
	}
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}
	return false
}

// handleConn services an individual TCP connection.
func (s *Service) handleConn(conn net.Conn) {
	// Ensure connection is closed when service is closed.
	defer func() {
		defer conn.Close()
		s.Logger.Info(fmt.Sprintf("close remote connection from %+v", conn.RemoteAddr()))
	}()
	s.Logger.Info(fmt.Sprintf("accept remote connection from %+v", conn.RemoteAddr()))
	requestReader := &request.ClusterMessageReader{}
	for {
		select {
		case <-s.closing:
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		req, err := requestReader.Read(conn)
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if canContinue(err) {
				continue
			}
			if err != io.EOF {
				s.Logger.Error("read error", zap.Error(err))
			}
			break
		}
		if req == nil {
			continue
		}
		if req.Type == createIteratorRequestMessage {
			// iterator is different from other requests
			ioError := s.processCreateIteratorRequest(conn, req.Data)
			if ioError {
				break
			}
			continue
		}
		respType, resp, err := s.handleRequest(req.Type, req.Data)
		if resp == nil {
			continue
		}
		err = s.writeClusterResponse(conn, respType, resp)
		if err != nil {
			// close conn due to error in writing
			break
		}
	}
}

func (s *Service) executeStatement(stmt influxql.Statement, database string) error {
	switch t := stmt.(type) {
	case *influxql.DropDatabaseStatement:
		return s.TSDBStore.DeleteDatabase(t.Name)
	case *influxql.DropMeasurementStatement:
		return s.TSDBStore.DeleteMeasurement(database, t.Name)
	case *influxql.DropSeriesStatement:
		return s.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
	case *influxql.DropRetentionPolicyStatement:
		return s.TSDBStore.DeleteRetentionPolicy(database, t.Name)
	default:
		return fmt.Errorf("%q should not be executed across a cluster", stmt.String())
	}
}

func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return internal.Statistics(s.stats, tags)
}

// ReadType reads the type from a TLV record.
func ReadType(r io.Reader) (byte, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return 0, err
	}
	return typ[0], nil
}

// ReadLV reads the length-value from a TLV record.
func ReadLV(conn net.Conn, timeout time.Duration) ([]byte, error) {
	// Read the size of the message.
	var sz int64
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	if err := binary.Read(conn, binary.BigEndian, &sz); err != nil {
		return nil, err
	}

	if sz >= MaxMessageSize {
		return nil, fmt.Errorf("max message size of %d exceeded: %d", MaxMessageSize, sz)
	}

	if sz == 0 {
		// empty msg
		return []byte{}, nil
	}

	// Read the value.
	buf := make([]byte, sz)
	conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// WriteTLV writes a type-length-value record to w.
func WriteTLV(w io.Writer, typ byte, buf []byte) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}

// WriteType writes the type in a TLV record to w.
func WriteType(w io.Writer, typ byte) error {
	if _, err := w.Write([]byte{typ}); err != nil {
		return err
	}
	return nil
}

// WriteLV writes the length-value in a TLV record to w.
func WriteLV(w io.Writer, buf []byte) error {
	// Write the size of the message.
	if err := binary.Write(w, binary.BigEndian, int64(len(buf))); err != nil {
		return err
	}

	// Write the value.
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

// EncodeTLV encodes v to a binary format and writes the record-length-value record to w.
func EncodeTLV(w io.Writer, typ byte, v encoding.BinaryMarshaler) error {
	if err := WriteType(w, typ); err != nil {
		return err
	}
	if err := EncodeLV(w, v); err != nil {
		return err
	}
	return nil
}

// EncodeLV encodes v to a binary format and writes the length-value record to w.
func EncodeLV(w io.Writer, v encoding.BinaryMarshaler) error {
	buf, err := v.MarshalBinary()
	if err != nil {
		return err
	}

	if err := WriteLV(w, buf); err != nil {
		return err
	}
	return nil
}
