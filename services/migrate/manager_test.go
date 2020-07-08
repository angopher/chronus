package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCloser struct {
	fn func()
}

func (t testCloser) Close() error {
	t.fn()
	return nil
}

func TestManagerNew(t *testing.T) {
	m := NewManager(0)
	assert.Equal(t, 1, m.parallel)

	m = NewManager(2)
	assert.Equal(t, 2, m.parallel)

	m = NewManager(22222)
	assert.Equal(t, TASK_PARALLEL_MAX, m.parallel)
}

func TestManagerTasks(t *testing.T) {
	cnt := 0
	task1 := &Task{
		ShardId: 123,
		SrcHost: "a",
		Closer: testCloser{
			fn: func() {
				cnt++
			},
		},
	}
	task2 := &Task{
		ShardId: 1235,
		Closer: testCloser{
			fn: func() {
				cnt++
			},
		},
	}
	m := NewManager(1)

	assert.Equal(t, 0, len(m.Tasks()))

	// add task
	assert.Nil(t, m.Add(task1))

	assert.Equal(t, 1, len(m.Tasks()))
	assert.Equal(t, task1, m.Tasks()[0])

	// add another
	assert.Nil(t, m.Add(task2))

	assert.Equal(t, 2, len(m.Tasks()))

	assert.NotNil(t, m.pop())
	assert.NotNil(t, m.pop())
	assert.Nil(t, m.pop())

	// kill
	m.Kill(task1.ShardId, "")
	assert.Equal(t, 2, len(m.Tasks()))
	assert.Equal(t, 0, cnt)
	m.Kill(task1.ShardId, "a")
	assert.Equal(t, 1, len(m.Tasks()))
	assert.Equal(t, 1, cnt)

	// remove only
	m.Remove(task1.ShardId)
	assert.Equal(t, 1, len(m.Tasks()))
	assert.Equal(t, 1, cnt)
	m.Remove(task2.ShardId)
	assert.Equal(t, 0, len(m.Tasks()))
	assert.Equal(t, 1, cnt)
}
