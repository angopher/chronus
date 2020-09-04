package x

import (
	"io"
	"net"
	"sync"
	"time"
)

type EchoServer struct {
	network  string
	address  string
	listener net.Listener
	closing  chan int
	once     sync.Once
	wg       sync.WaitGroup
}

func NewEchoServer(network, addr string) *EchoServer {
	return &EchoServer{
		network: network,
		address: addr,
		closing: make(chan int),
	}
}

func (s *EchoServer) Start() error {
	listener, err := net.Listen(s.network, s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	go s.loop()
	return nil
}

func (s *EchoServer) Close() {
	s.once.Do(func() {
		close(s.closing)
		s.listener.Close()
		s.wg.Wait()
	})
}

func (s *EchoServer) loop() {
	defer s.Close()

	s.wg.Add(1)
	defer s.wg.Done()
LOOP:
	for {
		select {
		case <-s.closing:
			break LOOP
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				break LOOP
			}
			go s.connLoop(conn)
		}
	}
}

func (s *EchoServer) connLoop(conn net.Conn) {
	s.wg.Add(1)
	defer func() {
		conn.Close()
		s.wg.Done()
	}()
	var (
		buf = make([]byte, 1024)
		n   int
		err error
	)
LOOP:
	for {
		select {
		case <-s.closing:
			break LOOP
		default:
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, err = conn.Read(buf)
			conn.SetDeadline(time.Time{})
		}
		if err == io.EOF {
			break LOOP
		}
		if err != nil {
			continue
		}
		if n < 1 {
			continue
		}
		if write(conn, buf[:n]) != nil {
			break LOOP
		}
	}
}

func write(conn net.Conn, data []byte) error {
	size := len(data)
	pos := 0
	for pos < size {
		n, err := conn.Write(data[pos:size])
		if err != nil {
			return err
		}
		pos += n
	}
	return nil
}
