package request_test

import (
	"errors"
	"testing"

	"github.com/angopher/chronus/coordinator/request"
	"github.com/stretchr/testify/assert"
)

type readFn func(p []byte) (n int, err error)

type mockReader struct {
	reader readFn
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	return r.reader(p)
}

func TestRequestReaderTimeout(t *testing.T) {
	reader := &request.ClusterMessageReader{}
	req, err := reader.Read(&mockReader{
		reader: func(p []byte) (n int, err error) {
			return 0, errors.New("timeout")
		},
	})
	assert.Nil(t, req)
	assert.NotNil(t, err)
	assert.Contains(t, "timeout", err.Error())
}

func TestRequestReaderPartial(t *testing.T) {
	step := 0
	reader := &request.ClusterMessageReader{}
	req, err := reader.Read(&mockReader{
		reader: func(p []byte) (n int, err error) {
			switch step {
			case 0:
				p[0] = 1
				p[1] = 0
				step++
				return 2, nil
			case 1:
				p[0] = 0
				p[1] = 0
				p[2] = 0
				p[3] = 0
				p[4] = 0
				step++
				return 5, nil
			}
			return 0, errors.New("timeout")
		},
	})
	assert.Nil(t, req)
	assert.NotNil(t, err)
}

func TestRequestReaderNormal(t *testing.T) {
	step := 0
	reader := &request.ClusterMessageReader{}
	req, err := reader.Read(&mockReader{
		reader: func(p []byte) (n int, err error) {
			switch step {
			case 0:
				p[0] = 1
				p[1] = 0
				step++
				return 2, nil
			case 1:
				p[0] = 0
				p[1] = 0
				p[2] = 0
				p[3] = 0
				p[4] = 0
				step++
				return 5, nil
			case 2:
				p[0] = 0
				p[1] = 2
				p[2] = 12
				step++
				return 3, nil
			case 3:
				p[0] = 34
				p[1] = 22
				step++
				return 2, errors.New("timeout")
			}
			return 0, errors.New("timeout")
		},
	})
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(1), req.Type)
	assert.Equal(t, []byte{12, 34}, req.Data)
	assert.Equal(t, []byte{22}, reader.Data())
}

func TestRequestReaderMore(t *testing.T) {
	step := 0
	reader := &request.ClusterMessageReader{}
	r := &mockReader{
		reader: func(p []byte) (n int, err error) {
			switch step {
			case 0:
				p[0] = 1
				p[1] = 0
				step++
				return 2, nil
			case 1:
				p[0] = 0
				p[1] = 0
				p[2] = 0
				p[3] = 0
				p[4] = 0
				step++
				return 5, nil
			case 2:
				p[0] = 0
				p[1] = 2
				p[2] = 12
				step++
				return 3, nil
			case 3:
				p[0] = 34
				p[1] = 22
				p[2] = 0
				step++
				return 3, errors.New("timeout")
			case 4:
				p[0] = 0
				p[1] = 0
				p[2] = 0
				p[3] = 0
				p[4] = 0
				p[5] = 0
				p[6] = 0
				step++
				return 7, nil
			case 5:
				n := copy(p, []byte{
					1, 0, 0, 0, 0, 0, 0, 0, 0,
					1, 0, 0, 0, 0, 0, 0, 0, 0,
					1, 0, 0, 0, 0, 0, 0, 0, 0,
				})
				step++
				return n, nil
			}
			return 0, errors.New("timeout")
		},
	}
	req, err := reader.Read(r)
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(1), req.Type)
	assert.Equal(t, []byte{12, 34}, req.Data)

	req, err = reader.Read(r)
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(22), req.Type)
	assert.Equal(t, 0, len(req.Data))

	req, err = reader.Read(r)
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(1), req.Type)
	assert.Equal(t, 0, len(req.Data))
	req, err = reader.Read(r)
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(1), req.Type)
	assert.Equal(t, 0, len(req.Data))
	req, err = reader.Read(r)
	assert.NotNil(t, req)
	assert.Nil(t, err)
	assert.Equal(t, byte(1), req.Type)
	assert.Equal(t, 0, len(req.Data))
}
