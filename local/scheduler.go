package local

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/oekit/pkg/timex/cron"
)

// ErrDuplicateSpec 表示同一个 cron 表达式被重复注册。
var ErrDuplicateSpec = errors.New("local: cron spec already scheduled")

// Task 是被 cron 触发的任务函数。
//
// at 为本次触发的计划时间（cron 计算出的到期点），可用于日志与幂等键。
// ctx 是调度器的运行上下文（Start 的 ctx），取消后任务应尽快返回。
//
// 任务返回错误只记日志、不重试：需要重试语义请在任务内部实现，
// 或改用 github.com/oekit/timertask 的 LocalBackend 延迟队列。
type Task func(ctx context.Context, at time.Time) error

// CatchUpPolicy 是错过触发点的补偿策略。
type CatchUpPolicy int

const (
	// CatchUpSkip（默认）：只执行最近的一个触发点。
	//
	// 适用于「状态收敛型」任务（对账、刷新、巡检）——历史触发点已过时，
	// 补跑没有意义，甚至有害（重复处理旧状态）。
	CatchUpSkip CatchUpPolicy = iota
	// CatchUpAll：所有错过的触发点按时间顺序逐一补跑。
	//
	// 适用于「计数型」任务（每次触发都代表一次必须发生的动作）。
	// 注意补跑会积压：任务执行慢于触发周期时，积压会持续增长。
	CatchUpAll
)

// 默认配置。
const (
	// defaultConcurrency 默认并发度（1 = 严格串行）。
	defaultConcurrency = 1
	// defaultMaxCatchUp 单次补跑的最大触发点数（防止时钟大跳变时无限枚举）。
	defaultMaxCatchUp = 1000
)

// Config 是 Scheduler 配置。
type Config struct {
	// Concurrency 全局任务并发度（同时执行的任务数上限），<=0 取 1（严格串行）。
	//
	// 同一 spec 的触发始终串行（自身不重叠）；不同 spec 之间按此上限并发。
	Concurrency int
	// CatchUp 错过触发点的补偿策略，零值取 CatchUpSkip。
	CatchUp CatchUpPolicy
	// Clock 时钟注入，nil 取 SystemClock{}；测试可注入 FakeClock。
	Clock Clock
	// Logger 日志器，nil 取 slog.Default()。
	Logger *slog.Logger
	// Location cron 表达式的时区，nil 取 time.Local。
	Location *time.Location
	// MaxCatchUp 单次补跑的最大触发点数，<=0 取 1000。
	MaxCatchUp int
}

// Scheduler 是基于 cron 表达式的本地调度器。
//
// 零外部中间件、零 Redis 依赖：只依赖标准库与 github.com/oekit/pkg/timex/cron
// （cron 表达式解析复用 oekit 矩阵的实现，不重复造轮子）。
//
// 典型用法：
//
//	sched, err := local.New(local.Config{Concurrency: 4})
//	if err != nil {
//		return err
//	}
//	if err := sched.Schedule("*/5 * * * * *", func(ctx context.Context, at time.Time) error {
//		return doSomething(ctx)
//	}); err != nil {
//		return err
//	}
//	go func() { _ = sched.Start(ctx) }()
//	defer sched.Stop(context.Background())
type Scheduler struct {
	cfg   Config
	log   *slog.Logger
	clock Clock

	mu      sync.Mutex
	entries map[string]*entry
	started bool
	stopped bool
	taskCtx context.Context
	active  int
	doneCh  chan struct{}

	stopCh   chan struct{}
	stopOnce sync.Once
	sem      chan struct{}
	wake     chan struct{}
}

// New 创建调度器。
func New(cfg Config) (*Scheduler, error) {
	switch cfg.CatchUp {
	case CatchUpSkip, CatchUpAll:
	default:
		return nil, fmt.Errorf("local: invalid catch-up policy %d", int(cfg.CatchUp))
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.MaxCatchUp <= 0 {
		cfg.MaxCatchUp = defaultMaxCatchUp
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Scheduler{
		cfg:     cfg,
		log:     cfg.Logger,
		clock:   cfg.Clock,
		entries: make(map[string]*entry),
		stopCh:  make(chan struct{}),
		sem:     make(chan struct{}, cfg.Concurrency),
		wake:    make(chan struct{}, 1),
	}, nil
}

// Schedule 注册一个 cron 任务。
//
// spec 交给 github.com/oekit/pkg/timex/cron 解析：支持 5 字段（分 时 日 月 周）
// 与 6 字段（秒 分 时 日 月 周）格式，以及 @every/@daily/@hourly 等宏。
//
// 同一个 spec 重复注册返回 ErrDuplicateSpec（不静默覆盖：覆盖会悄悄丢掉
// 原处理函数，且调用方难以察觉）。要换处理函数请先 Unschedule。
func (s *Scheduler) Schedule(spec string, fn Task) error {
	if fn == nil {
		return errors.New("local: nil task")
	}
	sched, err := cron.Parse(spec)
	if err != nil {
		return fmt.Errorf("local: parse cron %q: %w", spec, err)
	}
	if loc := s.cfg.Location; loc != nil {
		if ss, ok := sched.(*cron.SpecSchedule); ok {
			ss.Location = loc
		}
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("local: scheduler stopped")
	}
	if _, ok := s.entries[spec]; ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrDuplicateSpec, spec)
	}
	e := &entry{
		spec:     spec,
		schedule: sched,
		task:     fn,
		lastFire: s.clock.Now(),
	}
	e.cond = sync.NewCond(&e.mu)
	s.entries[spec] = e
	started := s.started
	if started {
		s.addRunnerLocked()
	}
	s.mu.Unlock()

	if started {
		goSafe(func() { s.runEntry(e) })
	}
	s.notify()
	return nil
}

// Unschedule 注销一个 cron 任务，返回是否确实移除了该任务。
//
// 正在执行的任务不会被中断（任务体可自行感知 ctx 取消）；未开始的触发被丢弃。
func (s *Scheduler) Unschedule(spec string) bool {
	s.mu.Lock()
	e, ok := s.entries[spec]
	if ok {
		delete(s.entries, spec)
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	e.close()
	s.notify()
	return true
}

// Start 启动调度循环并阻塞，直到 ctx 取消或 Stop 被调用。
//
// 返回时调度器已停止：不再产生新触发；正在执行的任务不会被强制中断
// （任务可通过 ctx 感知取消并自行退出）。重复调用返回错误。
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("local: scheduler already started")
	}
	if s.stopped {
		s.mu.Unlock()
		return errors.New("local: scheduler stopped")
	}
	s.started = true
	s.taskCtx = ctx
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	for range entries {
		s.addRunnerLocked()
	}
	s.mu.Unlock()

	for _, e := range entries {
		goSafe(func() { s.runEntry(e) })
	}
	goSafe(func() { s.run(ctx) })

	select {
	case <-ctx.Done():
	case <-s.stopCh:
	}
	s.shutdown()
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

// Stop 停止调度并等待正在执行的任务结束。
//
// 语义：
//   - 未开始的触发被丢弃；
//   - 正在执行的任务不被打断（任务的 ctx 不被取消），Stop 最多等待到 ctx 截止；
//   - ctx 超时返回 ctx.Err()，任务继续跑完（Go 无法安全中断 goroutine）。
//
// 可重复调用。
func (s *Scheduler) Stop(ctx context.Context) error {
	s.shutdown()

	s.mu.Lock()
	done := s.doneCh
	s.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdown 停止调度（幂等）：关闭停止信号与所有 entry，丢弃未开始的触发。
func (s *Scheduler) shutdown() {
	s.stopOnce.Do(func() { close(s.stopCh) })

	s.mu.Lock()
	s.stopped = true
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.entries = make(map[string]*entry)
	s.mu.Unlock()

	for _, e := range entries {
		e.close()
	}
	s.notify()
}

// run 是调度循环：计算到期触发点并派发，然后等待下一个触发点。
func (s *Scheduler) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		now := s.clock.Now()
		next := s.dispatchDue(now)

		if next.IsZero() {
			// 没有已注册任务：等新任务注册或调度停止
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-s.wake:
			}
			continue
		}

		wait := next.Sub(now)
		if wait < 0 {
			wait = 0
		}
		ch := s.clock.After(wait)
		// 防时钟在「读取 now → 注册等待」之间推进：重新核对，错过就立即重算。
		if !s.clock.Now().Before(next) {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-s.wake:
		case <-ch:
		}
	}
}

// dispatchDue 计算并派发所有已到期的触发点，返回下一个触发点时间（零值表示暂无）。
func (s *Scheduler) dispatchDue(now time.Time) time.Time {
	s.mu.Lock()
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	var next time.Time
	for _, e := range entries {
		points, capped := e.takeDue(now, s.cfg.CatchUp, s.cfg.MaxCatchUp)
		if capped {
			s.log.Warn("too many missed trigger points, truncated",
				"spec", e.spec, "max", s.cfg.MaxCatchUp)
		}
		if len(points) > 0 {
			s.log.Debug("cron fired", "spec", e.spec, "count", len(points), "at", points[0])
			e.enqueue(points, s.cfg.CatchUp)
		}
		if n := e.nextAfter(); !n.IsZero() && (next.IsZero() || n.Before(next)) {
			next = n
		}
	}
	return next
}

// runEntry 是单个 cron 任务的执行循环：同一 spec 严格串行。
func (s *Scheduler) runEntry(e *entry) {
	defer s.runnerDone()
	for {
		at, ok := e.nextPending()
		if !ok {
			return
		}
		s.execute(e, at)
	}
}

// execute 执行一次触发：先拿全局并发度配额，再跑任务。
func (s *Scheduler) execute(e *entry, at time.Time) {
	s.mu.Lock()
	ctx := s.taskCtx
	s.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-s.sem }()

	if err := s.invoke(ctx, e, at); err != nil {
		s.log.Error("cron task failed", "spec", e.spec, "at", at, "err", err)
	}
}

// invoke 调用任务并把 panic 收编为错误（panic 不得打死 runner goroutine）。
func (s *Scheduler) invoke(ctx context.Context, e *entry, at time.Time) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task panic: %v", r)
		}
	}()
	return e.task(ctx, at)
}

// notify 唤醒调度循环（注册/注销任务后调用）。
func (s *Scheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// pendingCount 返回各 spec 待执行触发点的总数（测试观测用）。
func (s *Scheduler) pendingCount() int {
	s.mu.Lock()
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	total := 0
	for _, e := range entries {
		e.mu.Lock()
		total += len(e.pending)
		e.mu.Unlock()
	}
	return total
}

// isStarted 返回调度器是否已启动（测试观测用）。
func (s *Scheduler) isStarted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// addRunnerLocked 登记一个 runner goroutine（调用方须持有 s.mu）。
func (s *Scheduler) addRunnerLocked() {
	if s.active == 0 {
		s.doneCh = make(chan struct{})
	}
	s.active++
}

// runnerDone 注销一个 runner goroutine；全部退出时关闭 doneCh。
func (s *Scheduler) runnerDone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active == 0 && s.doneCh != nil {
		close(s.doneCh)
		s.doneCh = nil
	}
}

// entry 是一个已注册的 cron 任务。
type entry struct {
	spec     string
	schedule cron.Schedule
	task     Task

	mu       sync.Mutex
	cond     *sync.Cond
	lastFire time.Time
	pending  []time.Time
	closed   bool
}

// takeDue 计算自上次触发以来已到期的触发点，并推进 lastFire。
//
// CatchUpSkip 只保留最近的一个触发点（其余跳过）；CatchUpAll 全部保留。
// 枚举上限 max 防止时钟大跳变时无限枚举（此时返回 capped=true）。
func (e *entry) takeDue(now time.Time, policy CatchUpPolicy, max int) ([]time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, false
	}

	var points []time.Time
	t := e.lastFire
	for i := 0; i < max; i++ {
		nxt := e.schedule.Next(t)
		if nxt.IsZero() || nxt.After(now) {
			break
		}
		points = append(points, nxt)
		t = nxt
	}
	if len(points) == 0 {
		return nil, false
	}
	e.lastFire = points[len(points)-1]
	capped := false
	if len(points) == max {
		// 达到枚举上限：可能还有更多错过的触发点（留给下一轮继续补）
		nxt := e.schedule.Next(t)
		capped = !nxt.IsZero() && !nxt.After(now)
	}
	if policy == CatchUpSkip && len(points) > 1 {
		points = points[len(points)-1:]
	}
	return points, capped
}

// nextAfter 返回下一个（未来的）触发点时间；零值表示无法计算。
func (e *entry) nextAfter() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return time.Time{}
	}
	return e.schedule.Next(e.lastFire)
}

// enqueue 把触发点放入待执行队列。
//
// CatchUpSkip：新触发点替换旧的待执行触发（同一 spec 最多积压 1 个），
// 保证「上一轮还没跑完又到点」不会无限堆积。
// CatchUpAll：按时间顺序追加，全部补跑。
func (e *entry) enqueue(points []time.Time, policy CatchUpPolicy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if policy == CatchUpSkip {
		e.pending = e.pending[:0]
	}
	e.pending = append(e.pending, points...)
	e.cond.Signal()
}

// nextPending 阻塞等待下一个待执行触发点；entry 关闭时返回 false。
func (e *entry) nextPending() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for len(e.pending) == 0 && !e.closed {
		e.cond.Wait()
	}
	if e.closed {
		return time.Time{}, false
	}
	at := e.pending[0]
	e.pending = e.pending[1:]
	return at, true
}

// close 关闭 entry：丢弃未开始的触发，唤醒等待中的 runner（幂等）。
func (e *entry) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	e.pending = nil
	e.cond.Broadcast()
}
