package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	cfg := NewConfig()
	assert.NotNil(t, cfg.Validate())
	cfg.Dir = "some_value"
	assert.Nil(t, cfg.Validate())

	diag, err := cfg.Diagnostics()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(diag.Rows))
	assert.Equal(t, 1, len(diag.Rows[0]))
	assert.Equal(t, "some_value", diag.Rows[0][0])
}
