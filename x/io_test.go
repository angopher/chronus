package x

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExists(t *testing.T) {
	assert.Equal(t, NotExisted, Exists(""))
	assert.Equal(t, NotExisted, Exists(".........."))
	assert.Equal(t, NotExisted, Exists("/asdf/asdf/asdf/sad/f/sadf/"))
	assert.Equal(t, NotExisted, Exists("/asdf/asdf/asdf/sad/f/sadf/a"))

	tmp := os.TempDir() + "test/"
	tmpFile := tmp + "a"
	assert.Equal(t, NotExisted, Exists(tmp))
	assert.Equal(t, NotExisted, Exists(tmpFile))

	os.MkdirAll(tmp, os.FileMode(0755))
	defer os.RemoveAll(tmp)
	os.Create(tmpFile)
	assert.Equal(t, ExistDir, Exists(tmp))
	assert.Equal(t, ExistFile, Exists(tmpFile))
}
