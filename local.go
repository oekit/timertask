package timertask

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/yi-nology/ekit-concurrency/async"
)

// localTaskItem 本地任务条目（用于优先队列）。
type localTaskItem struct {
	task     *Task
	index    int
	execTime int64
}

// priorityQueue 任务优先队列（按执行时间排序）。
type priorityQueue []*localTaskItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].execTime < pq[j].execTime
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*localTaskItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// LocalBackend 是基于内存的本地延迟任务实现。
// 适用于单实例场景或开发测试环境。
type LocalBackend struct {
	handler    Handler
	workers    int
	retryCount int
	retryDelay time.Duration
	onFailure  func(*Task, error)

	// 优先队列
	mu      sync.Mutex
	queue   priorityQueue
	taskMap map[string]*localTaskItem

	// 本地队列
	localQueue chan *Task
	stopChan   chan struct{}
	stopOnce   sync.Once
}

// NewLocalBackend 创建基于内存的本地延迟任务后端。
//
//   - handler: 任务处理函数
func NewLocalBackend(handler Handler, opts ...TimerTaskOption) *LocalBackend {
	cfg := applyOptions(opts)
	b := &LocalBackend{
		handler:    handler,
		workers:    cfg.workers,
		retryCount: cfg.retryCount,
		retryDelay: cfg.retryDelay,
		onFailure:  cfg.onFailure,
		queue:      make(priorityQueue, 0),
		taskMap:    make(map[string]*localTaskItem),
		localQueue: make(chan *Task, 10000),
		stopChan:   make(chan struct{}),
	}

	heap.Init(&b.queue)

	return b
}

// Send 提交一个延迟任务。
// task.EndTime（Unix 秒）决定任务何时被执行。
func (b *LocalBackend) Send(ctx context.Context, task *Task) error {
	if task.CreateTime == 0 {
		task.CreateTime = time.Now().Unix()
	}
	if task.MaxRetry == 0 {
		task.MaxRetry = b.retryCount
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 检查任务是否已存在
	if _, exists := b.taskMap[task.ID]; exists {
		return nil
	}

	item := &localTaskItem{
		task:     task,
		execTime: task.EndTime,
	}

	heap.Push(&b.queue, item)
	b.taskMap[task.ID] = item

	log.Info("task scheduled", "taskID", task.ID, "endTime", task.EndTime)
	return nil
}

// Start 启动任务消费（阻塞）。
// ctx 取消时所有 goroutine 退出。
func (b *LocalBackend) Start(ctx context.Context) error {
	// 启动扫描器
	async.GoSafe(func() { b.scanLoop(ctx) })

	// 启动本地 Worker 池
	for i := 0; i < b.workers; i++ {
		async.GoSafe(func() { b.workerLoop(ctx) })
	}

	<-ctx.Done()
	// stopOnce 防二次 Start 后重复 close panic(2026-09-06 复审加固);
	// Local 侧任务为 at-most-once:localQueue 阻塞中遇停机的任务直接丢弃、
	// 无回灌(与 Redis 侧 doc 声明的语义对齐,进程存活期内不丢已执行任务)
	b.stopOnce.Do(func() { close(b.stopChan) })
	return ctx.Err()
}

// scanLoop 定期扫描到期任务并推入本地队列。
func (b *LocalBackend) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		case <-ticker.C:
			b.scanOnce(ctx)
		}
	}
}

func (b *LocalBackend) scanOnce(ctx context.Context) {
	now := time.Now().Unix()

	b.mu.Lock()
	defer b.mu.Unlock()

	for b.queue.Len() > 0 {
		item := b.queue[0]
		if item.execTime > now {
			break
		}

		// 弹出到期任务
		heap.Pop(&b.queue)
		delete(b.taskMap, item.task.ID)

		// 推入本地队列
		select {
		case b.localQueue <- item.task:
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		}
	}
}

// workerLoop 从本地队列消费任务。
func (b *LocalBackend) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		case task := <-b.localQueue:
			if task == nil {
				continue
			}
			b.processTask(ctx, task)
		}
	}
}

// processTask 处理单个任务，支持重试。
func (b *LocalBackend) processTask(ctx context.Context, task *Task) {
	var err error
	normalizeRetry(task)
	// retryTimer lazy init：见 redis.go 对应注释。
	// defer 在 if 块里只触发 1 次（nil-check 失败后 retryTimer != nil），
	// 不会在循环里累积，gocritic 的 deferInLoop 是误报。
	var retryTimer *time.Timer
	for i := 0; i <= task.MaxRetry; i++ {
		if i > 0 {
			// 重试延迟
			if retryTimer == nil {
				retryTimer = time.NewTimer(b.retryDelay)
				//nolint:gocritic // lazy init：仅首次进入重试路径时分配，defer 与 NewTimer 配对
				defer retryTimer.Stop()
			} else {
				retryTimer.Reset(b.retryDelay)
			}
			select {
			case <-ctx.Done():
				return
			case <-b.stopChan:
				return
			case <-retryTimer.C:
			}
			task.RetryCount = i
			log.Warn("retrying task", "taskID", task.ID, "attempt", i, "maxRetry", task.MaxRetry)
		}

		// handler panic 就地收编为任务失败，走重试/onFailure 路径（见 runHandlerSafe）
		err = runHandlerSafe(ctx, b.handler, task)
		if err == nil {
			return
		}
		log.Error("handler error", "taskID", task.ID, "err", err)
	}

	// 重试次数用完，记录失败
	log.Error("task failed after retries", "taskID", task.ID, "maxRetry", task.MaxRetry, "err", err)
	if b.onFailure != nil {
		b.onFailure(task, err)
	}
}

// PendingCount 返回队列中待处理任务数。
func (b *LocalBackend) PendingCount(ctx context.Context) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(b.queue.Len()), nil
}

// LocalPendingCount 返回本地队列中待处理任务数。
func (b *LocalBackend) LocalPendingCount() int {
	return len(b.localQueue)
}
