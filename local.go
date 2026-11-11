package timertask

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/oekit/timertask/internal/gosafe"
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
	clock      Clock // #78：时间判断单源；nil 取 SystemClock{}

	// 优先队列
	mu      sync.Mutex
	queue   priorityQueue
	taskMap map[string]*localTaskItem
	// inflight 记录「已弹出优先队列、尚在执行窗口内」的任务 ID（#98）：
	// 同 ID 去重不再只覆盖排队态——scanOnce 弹出时占用、processTask
	// 返回时释放，执行窗口内重复 Send 一样不入队。
	inflight map[string]struct{}

	// 本地队列
	localQueue chan *Task
	stopChan   chan struct{}
	stopOnce   sync.Once
	// started/stopped 是 Start 的状态机（#124）：started 在 Start 入口登记，
	// stopped 在停机收尾登记，由 b.mu 保护。stopChan 关闭后没有任何路径重建
	// 它——若不做状态机，二次 Start 拉起的 goroutine 会全部立刻命中已关闭的
	// stopChan 退出（哑火），任务照常入堆但零消费，是比 panic 更隐蔽的静默
	// 失效。状态机让「第二次 Start」要么报错、要么（首次）真的开始消费。
	started bool
	stopped bool
}

// NewLocalBackend 创建基于内存的本地延迟任务后端。
//
//   - handler: 任务处理函数
func NewLocalBackend(handler Handler, opts ...TimerTaskOption) *LocalBackend {
	cfg := applyOptions(opts)
	clock := cfg.clock
	if clock == nil {
		clock = SystemClock{}
	}
	b := &LocalBackend{
		handler:    handler,
		workers:    cfg.workers,
		retryCount: cfg.retryCount,
		retryDelay: cfg.retryDelay,
		onFailure:  cfg.onFailure,
		clock:      clock,
		queue:      make(priorityQueue, 0),
		taskMap:    make(map[string]*localTaskItem),
		inflight:   make(map[string]struct{}),
		localQueue: make(chan *Task, 10000),
		stopChan:   make(chan struct{}),
	}

	heap.Init(&b.queue)

	return b
}

// Send 提交一个延迟任务。
// task.EndTime（Unix 秒）决定任务何时被执行。
//
// 语义：
//   - task.ID 为空 ⇒ 返回 ErrEmptyTaskID（ID 必须唯一，空 ID 无法区分任务）。
//   - 同 ID 重复提交 ⇒ **不入队**（先到先得），返回 nil（视为幂等提交成功）。
//     占用窗口覆盖排队态**与执行窗口**（#98）：任务被扫描弹出进入执行后、
//     处理结束前，同 ID Send 同样不入队——调用方依赖该幂等语义防重不会在
//     执行窗口内失效。处理结束后 ID 释放，可重新提交。
//   - 不改写调用方的 Task（#97）：CreateTime/MaxRetry 的缺省补齐、worker
//     重试时对 RetryCount 的推进都落在引擎内部的副本上，调用方提交后继续
//     读/复用自己的 Task 结构不会看到字段被引擎改写。
//   - 后端停机后（Start 已返回）⇒ 返回 ErrBackendStopped（#124），任务
//     不入堆——继续入堆会让 PendingCount 持续增长而永无消费，运维会误判
//     为队列积压。
func (b *LocalBackend) Send(ctx context.Context, task *Task) error {
	if task.ID == "" {
		return ErrEmptyTaskID
	}
	// #97：入队前深拷贝（Task 全为值字段，浅拷贝即全量拷贝）——缺省补齐与
	// worker 侧改写都落在副本上，调用方对象保持提交时的原样。
	c := *task
	if c.CreateTime == 0 {
		c.CreateTime = b.now().Unix()
	}
	if c.MaxRetry == 0 {
		c.MaxRetry = b.retryCount
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// #124：停机后拒绝新任务（见 Send 注释的任务不入堆条目）。
	if b.stopped {
		return ErrBackendStopped
	}

	// 检查任务是否已存在（排队态）
	if _, exists := b.taskMap[task.ID]; exists {
		logger.Info("task already scheduled, skipped", "taskID", task.ID)
		return nil
	}
	// 检查任务是否在执行窗口内（#98）
	if _, busy := b.inflight[task.ID]; busy {
		logger.Info("task already in flight, skipped", "taskID", task.ID)
		return nil
	}

	item := &localTaskItem{
		task:     &c,
		execTime: c.EndTime,
	}

	heap.Push(&b.queue, item)
	b.taskMap[task.ID] = item

	logger.Info("task scheduled", "taskID", task.ID, "endTime", task.EndTime)
	return nil
}

// Start 启动任务消费（阻塞）。
// ctx 取消时所有 goroutine 退出。
//
// 实例一次性（#124）：Start 返回后实例进入停机态——再次调用返回
// ErrBackendStopped（首个 Start 尚未返回时的并发重复调用返回 already
// started 错误），不会像旧实现那样拉起一组因 stopChan 已关闭而立刻退出的
// 哑火 goroutine。需要重启任务消费请重建实例。
func (b *LocalBackend) Start(ctx context.Context) error {
	// #124 状态机：入口先做状态检查并登记 started（与 Send/PendingCount 共用
	// b.mu）。检查必须在拉起 goroutine 之前，否则二次调用同样会拉起一组必然
	// 哑火的消费者。
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return ErrBackendStopped
	}
	if b.started {
		b.mu.Unlock()
		return ErrBackendAlreadyStarted
	}
	b.started = true
	b.mu.Unlock()

	// 启动扫描器
	gosafe.Go(func() { b.scanLoop(ctx) })

	// 启动本地 Worker 池
	for range b.workers {
		gosafe.Go(func() { b.workerLoop(ctx) })
	}

	<-ctx.Done()
	// stopOnce 防二次 Start 后重复 close panic(2026-09-06 复审加固);
	// Local 侧任务为 at-most-once:localQueue 阻塞中遇停机的任务直接丢弃、
	// 无回灌(与 Redis 侧 doc 声明的语义对齐,进程存活期内不丢已执行任务)
	b.stopOnce.Do(func() { close(b.stopChan) })
	// #124：登记停机——此后的二次 Start / Send 都显式报错（ErrBackendStopped）。
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
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

// now 返回当前时间（#78 时钟注入单源）。
func (b *LocalBackend) now() time.Time { return b.clock.Now() }

func (b *LocalBackend) scanOnce(ctx context.Context) {
	now := b.now().Unix()

	// #92/#94：锁内只 pop 到期任务，推入 localQueue 的发送在**锁外**做。
	// 此前发送在 b.mu 保护内：localQueue（容量 10000）涨满且 worker 全卡在
	// 慢 handler 时，select 阻塞在发送上但仍持锁 ⇒ Send/PendingCount 全部
	// 堵在 b.mu 上——互斥量被无限期占住，加锁互斥量整体卡死。
	b.mu.Lock()
	var due []*localTaskItem
	for b.queue.Len() > 0 {
		item := b.queue[0]
		if item.execTime > now {
			break
		}
		heap.Pop(&b.queue)
		delete(b.taskMap, item.task.ID)
		// #98：与 delete(taskMap) 同一临界段内占用 inflight——弹出与占用
		// 之间没有窗口，执行窗口内的同 ID Send 必然被去重挡住。
		b.inflight[item.task.ID] = struct{}{}
		due = append(due, item)
	}
	b.mu.Unlock()

	for i, item := range due {
		select {
		case b.localQueue <- item.task:
		case <-ctx.Done():
			// 未送达的剩余任务释放 ID 占用（at-most-once：停机窗口内丢弃，
			// 与 Start 的停机语义一致）
			b.releaseInflight(due[i:])
			return
		case <-b.stopChan:
			b.releaseInflight(due[i:])
			return
		}
	}
}

// releaseInflight 释放 in-flight ID 占用（#98）：items 为尚未送达 localQueue
// 的剩余弹出任务。
func (b *LocalBackend) releaseInflight(items []*localTaskItem) {
	b.mu.Lock()
	for _, item := range items {
		delete(b.inflight, item.task.ID)
	}
	b.mu.Unlock()
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

// processTask 处理单个任务，支持重试（重试主循环单源 RunWithRetry，#169：
// 与 redis 后端平级复制的骨架收敛到根包；local 的差异只有「中断丢弃」，
// 即 OnInterrupted/Interrupted 钩子取 nil 缺省）。
func (b *LocalBackend) processTask(ctx context.Context, task *Task) {
	// #98：处理结束（成功 / 重试耗尽 / 中断返回）释放 ID 占用——此后同 ID
	// 可重新提交。被中断的任务按 at-most-once 语义丢弃，释放后调用方可重发。
	defer func() {
		b.mu.Lock()
		delete(b.inflight, task.ID)
		b.mu.Unlock()
	}()

	outcome, err := RunWithRetry(ctx, task, b.handler, b.retryDelay, b.stopChan, RetryHooks{
		OnRetry: func(attempt, maxRetry int) {
			logger.Warn("retrying task", "taskID", task.ID, "attempt", attempt, "maxRetry", maxRetry)
		},
		OnError: func(err error) {
			logger.Error("handler error", "taskID", task.ID, "err", err)
		},
	})
	switch outcome {
	case RetrySuccess, RetryInterrupted:
		// 成功 / 中断丢弃（at-most-once）。
		return
	case RetryExhausted:
		// 重试次数用完，记录失败
		logger.Error("task failed after retries", "taskID", task.ID, "maxRetry", task.MaxRetry, "err", err)
		if b.onFailure != nil {
			b.onFailure(task, err)
		}
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
