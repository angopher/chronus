package x

// CyclicBuffer is a buffer which refills cyclically after writes.
//	It can be compared to specific []byte efficiently.
//	Well, it's NOT THREAD SAFE.
type CyclicBuffer struct {
	buf []byte
	pos int
	cnt int
}

func NewCyclicBuffer(capacity int) *CyclicBuffer {
	return &CyclicBuffer{
		buf: make([]byte, capacity),
	}
}

func (c *CyclicBuffer) forward() {
	c.pos++
	if c.pos >= len(c.buf) {
		c.pos = 0
	}
}

func (c *CyclicBuffer) write(data []byte, offset int) {
	l := len(data)
	for i := offset; i < l; i++ {
		c.buf[c.pos] = data[i]
		c.forward()
	}
}

func (c *CyclicBuffer) Cap() int {
	return len(c.buf)
}

func (c *CyclicBuffer) Len() int {
	return c.cnt
}

// Dump dumps internal data into given slice and returns write byte count
//	ATTENTION: the given buffer should not smaller than buffer capacity
func (c *CyclicBuffer) Dump(data []byte) int {
	l := len(c.buf)
	if len(data) < l {
		return 0
	}
	posRead := c.pos
	posWrite := 0
	skip := len(c.buf) - c.cnt
	for i := 0; i < len(c.buf); i++ {
		if skip > 0 {
			skip--
			goto NEXT
		}
		data[posWrite] = c.buf[posRead]
		posWrite++
	NEXT:
		posRead++
		if posRead >= l {
			posRead %= l
		}
	}
	return posWrite
}

func (c *CyclicBuffer) Write(data []byte) {
	sz := len(data)
	if sz < 1 {
		return
	}
	c.cnt += sz
	offset := sz - len(c.buf)
	if offset < 0 {
		offset = 0
	}
	c.write(data, offset)
}

// Compare returns true when matched
func (c *CyclicBuffer) Compare(data []byte) bool {
	l0 := len(c.buf)
	l1 := len(data)
	if l0 != l1 {
		return false
	}
	pos := c.pos
	for i := 0; i < l1; i++ {
		if data[i] != c.buf[pos] {
			return false
		}
		pos++
		if pos >= l0 {
			pos %= l0
		}
		if pos == c.pos {
			break
		}
	}
	return true
}
