package migrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/pkg/tar"
	"github.com/stretchr/testify/assert"
)

func TestExecutorRestore(t *testing.T) {
	basePath := os.TempDir() + ".chronus/tmp"
	tarFilePath := os.TempDir() + ".chronus/t.tar"
	os.RemoveAll(basePath)
	assert.Nil(t, os.MkdirAll(basePath, os.FileMode(0755)))
	defer os.RemoveAll(basePath)

	// initial files: a, b
	f, err := os.Create(basePath + "/a")
	assert.Nil(t, err)
	f.WriteString("testA")
	f.Close()

	f, err = os.Create(basePath + "/b")
	assert.Nil(t, err)
	f.WriteString("testB")
	f.Close()

	os.Remove(tarFilePath)
	f, err = os.Create(tarFilePath)
	assert.Nil(t, err)
	defer os.Remove(tarFilePath)

	// create tar file
	err = tar.Stream(f, basePath, "/db/rp/1", nil)
	assert.Nil(t, err)
	f.Close()

	// verify files before removing
	arr, err := filepath.Glob(basePath + "/*")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(arr))

	// remove them
	os.Remove(basePath + "/a")
	os.Remove(basePath + "/b")

	// verify it's empty
	arr, err = filepath.Glob(basePath + "/*")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(arr))

	// extract tar file
	assert.Nil(t, restoreFromTar(tarFilePath, basePath))

	// verify it's comtent
	arr, err = filepath.Glob(basePath + "/1/*")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(arr))
}
