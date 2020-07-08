package migrate

import (
	"errors"
	"io"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	TASK_PARALLEL_MAX = 24
)

var (
	ErrTaskDuplicated = errors.New("Shard migration task has already existed")
)

// Task holds the task info
type Task struct {
	// State
	Started   bool
	Finished  bool
	StartTime int64 // timestamp in second
	Error     error
	C         chan error

	// Meta
	Database  string
	Retention string
	ShardId   uint64
	SrcHost   string

	// Store
	DstStorePath string
	TmpStorePath string

	// Progress
	Copied          uint64
	Limiter         *rate.Limiter
	ProgressLimiter *rate.Limiter // for progress logging

	// Callback
	Closer io.Closer
}

func (t *Task) succ() {
	t.Finished = true
	t.C <- nil
	t.Closer.Close()
}

func (t *Task) error(err error) {
	t.Error = err
	t.Finished = true
	t.C <- err
	t.Closer.Close()
}

// Manager is the container of tasks running or queued
//	You should use NewCopyManager to get an instance
type Manager struct {
	sync.Mutex
	wg         sync.WaitGroup
	cond       *sync.Cond
	taskMap    map[uint64]*Task
	taskQueue  []*Task
	parallel   int
	logger     *zap.Logger
	shouldStop bool
}

// NewManager creates and returns a new manager
func NewManager(parallel int) *Manager {
	if parallel < 1 {
		parallel = 1
	}
	if parallel > TASK_PARALLEL_MAX {
		parallel = TASK_PARALLEL_MAX
	}
	m := &Manager{
		parallel:  parallel,
		taskMap:   make(map[uint64]*Task),
		taskQueue: make([]*Task, 0, TASK_PARALLEL_MAX),
		cond:      sync.NewCond(&sync.Mutex{}),
	}
	return m
}

func (m *Manager) Start() {
	for i := 0; i < m.parallel; i++ {
		go m.loop()
	}
}

func (m *Manager) loop() {
	m.wg.Add(1)
	defer m.wg.Done()
	for !m.shouldStop {
		t := m.pop()
		if t == nil {
			m.cond.L.Lock()
			m.cond.Wait()
			m.cond.L.Unlock()
			continue
		}

		m.execute(t)
		m.Remove(t.ShardId)
	}
}

func (m *Manager) Close() {
	m.shouldStop = true
	m.cond.Broadcast()
	m.wg.Wait()
	m.logger.Info("Shutdown migrating manager")
}

func (m *Manager) WithLogger(log *zap.Logger) {
	m.logger = log.With(zap.String("service", "migrate.Manager"))
}

func (m *Manager) pop() *Task {
	m.Lock()
	defer m.Unlock()
	if len(m.taskQueue) == 0 {
		return nil
	}
	t := m.taskQueue[len(m.taskQueue)-1]
	m.taskQueue = m.taskQueue[:len(m.taskQueue)-1]
	return t
}

// Add registers a task under specific shard and returns error if there was one already.
func (m *Manager) Add(task *Task) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.taskMap[task.ShardId]; ok {
		return ErrTaskDuplicated
	}
	task.C = make(chan error, 1)
	// Currently limit to 5MB/s, 10MB/s in burst
	task.Limiter = rate.NewLimiter(5*1024*1024, 10*1024*1024)
	task.ProgressLimiter = rate.NewLimiter(0.05, 10) // 1 log every 20 seconds
	m.taskMap[task.ShardId] = task
	m.taskQueue = append(m.taskQueue, task)
	m.cond.Signal()
	return nil
}

// Remove removes the task under specific shard id WITHOUT stopping it
func (m *Manager) Remove(id uint64) {
	m.Lock()
	defer m.Unlock()
	if t, ok := m.taskMap[id]; ok {
		delete(m.taskMap, id)
		m.removeFromQueue(t)
	}
}

// Tasks returns all tasks
func (m *Manager) Tasks() []*Task {
	m.Lock()
	defer m.Unlock()
	tasks := make([]*Task, 0, len(m.taskMap))
	for _, t := range m.taskMap {
		tasks = append(tasks, t)
	}
	return tasks
}

func (m *Manager) removeFromQueue(t *Task) {
	if t == nil {
		return
	}
	pos := -1
	for i, item := range m.taskQueue {
		if item == t {
			pos = i
			break
		}
	}
	if pos >= 0 {
		m.taskQueue = append(m.taskQueue[0:pos], m.taskQueue[pos+1:]...)
	}
}

// Kill kills the running task and remove it from list
//	Host addresses are required for double check
func (m *Manager) Kill(id uint64, srcHost string) {
	m.Lock()
	defer m.Unlock()
	t, ok := m.taskMap[id]
	if !ok {
		return
	}
	if srcHost == t.SrcHost {
		if t.Closer != nil {
			t.Closer.Close()
		}
		delete(m.taskMap, id)
		m.removeFromQueue(t)
	}
}
