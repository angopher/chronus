package request

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	MAX_BODY = 1024 * 1024 * 1024
)

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 64)
		},
	}
)

type ClusterMessage struct {
	Type byte
	Data []byte
}

type ClusterMessageReader struct {
	data []byte
}

func (reader *ClusterMessageReader) fetch() *ClusterMessage {
	sz := len(reader.data)
	if sz < 9 {
		return nil
	}
	l := binary.BigEndian.Uint64(reader.data[1:9])
	if l > MAX_BODY {
		panic(fmt.Sprint("Error decoding cluster request, got a huge size of ", l))
	}
	consumed := int(l + 8 + 1)
	if sz < consumed {
		// not enough
		return nil
	}

	data := make([]byte, l)
	copy(data, reader.data[9:consumed])
	req := &ClusterMessage{
		Type: reader.data[0],
		Data: data,
	}
	// shift the buffer
	for i := 0; i < sz-consumed; i++ {
		reader.data[i] = reader.data[i+consumed]
	}
	reader.data = reader.data[:sz-consumed]
	return req
}

func (reader *ClusterMessageReader) Read(r io.Reader) (*ClusterMessage, error) {
	if req := reader.fetch(); req != nil {
		return req, nil
	}
	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			reader.data = append(reader.data, buf[:n]...)
			if req := reader.fetch(); req != nil {
				return req, nil
			}
		}
		if err != nil {
			return nil, err
		}
	}
}

// Len returns the length of data in buffer
func (reader *ClusterMessageReader) Len() int {
	return len(reader.data)
}

// Data returns unprocessed data
func (reader *ClusterMessageReader) Data() []byte {
	if len(reader.data) == 0 {
		return []byte{}
	}
	data := make([]byte, len(reader.data))
	copy(data, reader.data)
	return data
}
