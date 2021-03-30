package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommandName(t *testing.T) {
	name, args := ParseCommandName([]string{"-wrong", "test"})
	assert.Empty(t, name)
	assert.Equal(t, 2, len(args))

	name, args = ParseCommandName([]string{})
	assert.Empty(t, name)
	assert.Equal(t, 0, len(args))

	name, args = ParseCommandName([]string{"-h", "config"})
	assert.Equal(t, "help", name)
	assert.Equal(t, 1, len(args))
	assert.Equal(t, "config", args[0])

	name, args = ParseCommandName([]string{"--help", "config"})
	assert.Equal(t, "help", name)
	assert.Equal(t, 1, len(args))
	assert.Equal(t, "config", args[0])

	name, args = ParseCommandName([]string{"config", "-c", "a.yml"})
	assert.Equal(t, "config", name)
	assert.Equal(t, 2, len(args))
	assert.Equal(t, "-c", args[0])
	assert.Equal(t, "a.yml", args[1])
}
