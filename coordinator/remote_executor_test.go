package coordinator

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// small buffer to test
func testWithBuffer(buf []byte) []byte {
	rd, wr := io.Pipe()
	defer rd.Close()
	go func() {
		defer wr.Close()
		wr.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		wr.Write([]byte{1, 2})
		fmt.Println("write end")
	}()
	var result []byte
	reader := newIteratorReader(rd, []byte{9, 10, 11, 12, 13, 14, 15, 16})
	for {
		n, err := reader.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			result = append(result, buf[:n]...)
		}
	}
	return result
}

func TestIteratorReader(t *testing.T) {
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, testWithBuffer(make([]byte, 3)))
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, testWithBuffer(make([]byte, 9)))
}

func TestRemoteNodeExecuterTagKeys(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterTagValues(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterMeasurementNames(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterSeriesCardinality(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterFieldDimensions(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterDeleteSeries(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterDeleteDatabase(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterDeleteMeasurement(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterIteratorCost(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterMapType(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterCreateIterator(t *testing.T) {
	//TODO
}

func TestRemoteNodeExecuterTaskManagerStatement(t *testing.T) {
	//TODO
}
