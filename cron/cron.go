package cron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pkgerrors "github.com/oekit/pkg/errors"

	"github.com/oekit/timertask"
	"github.com/oekit/timertask/internal/gosafe"
	"github.com/oekit/timertask/local"
)

// ErrSchedulerStopped 表示实例已停机（Close 已调用或 StartReconcile 的 ctx
// 已取消），进入终态——对齐根包 ErrBackendStopped 的实例一次性纪律（#124）：
// 停机后 Reconcile/Upsert/StartReconcile 返回本错误，不静默哑火。需要重启
// 请重建实例。
var ErrSchedulerStopped = errors.New("cron: scheduler stopped")

// Plan 是一条期望计划（DB 计划表单条的装配面镜像，#159 建议形态原样交付）。
type Plan struct {
	// ID 计划唯一 ID（业务任务 ID）。空 ID 使 Reconcile 整体 fail-closed。
	ID string
	// Spec cron 表达式（交 oekit/pkg/timex/cron 解析：5/6 字段 + @every
	// 等宏，与 local.Scheduler 同口径）。
	Spec string
	// Fingerprint 变更指纹——增量重装的唯一 diff 单元（对齐 zeus/task 的
	// taskFingerprint：表达式+状态+updatedAt 的哈希）：
	//
	//   - 指纹未变 ⇒ 幂等空操作（不重装、不重置触发基准——防 @every 被
	//     周期重装不断后移）；
	//   - 指纹变化 ⇒ 摘旧装新（计划被编辑过，从新基准起算）；
	//   - 留空 ⇒ 派生指纹 = Spec（库侧可见的行为面只有表达式）。
	//
	// 注意指纹构成是调用方的契约选择：含 updatedAt 时「改任务元数据」
	// 也会重装、@every 触发基准随之重置（刻意语义——编辑过就该从新基准
	// 起算）；只想「行为变更才重装」就把指纹定为 表达式+状态 的哈希。
	Fingerprint string
	// Enabled false = 停用：出现在期望集里时按注销收敛（不出现在期望集里
	// 同效——两种停用表达都收敛为不触发）。
	Enabled bool
}

// Executor 是计划触发的执行函数（#159 建议形态原样交付）。
//
// planID 为计划 ID；trigger 为触发点标识（RFC3339，秒精度）——幂等键/
// 互斥键/台账键三同源（execLockKey/RunRecord 同值），业务可直接拿去做
// 幂等去重。返回错误只记台账与日志（local.Scheduler「任务失败不重试」
// 同口径——需要重试语义请在任务内部实现）。
type Executor func(ctx context.Context, planID string, trigger string) error

// StaleSweepFunc 是启动清扫钩子（对齐 zeus/task 的 FailStaleRunning 形态）：
// 实例启动时把上次崩溃遗留的「执行中」记录收敛为失败（清扫后补发事件也
// 在钩子内完成），返回清扫条数。StartReconcile 在首次对账**之前**调用一次；
// 清扫失败只记日志不阻断启动（清扫不该挡住整个调度面，下轮对账兜底）。
type StaleSweepFunc func(ctx context.Context) (swept int, err error)

// CronOption 配置 Scheduler。命名遵循仓 AGENTS 的 XxxOption 纪律
// （禁止裸 Option，多包 co-import 冲突）——issue 建议形态的 `...Option`
// 相应加前缀。
type CronOption func(*cronOptions)

// cronOptions 是 Scheduler 的配置集合（New 时定版）。
type cronOptions struct {
	mutex       Mutex
	ledger      Ledger
	instance    string
	clock       local.Clock
	logger      *slog.Logger
	budget      RetryBudget
	staleSweep  StaleSweepFunc
	runCapacity int
	concurrency int
	catchUp     local.CatchUpPolicy
	location    *time.Location
}

// WithMutex 注入跨实例执行互斥（distlock 等，见 Mutex 的适配示例）。
// 不设置时只有进程内 slot 互斥（单实例部署即够；多实例部署不注入会双跑——
// 这是显式接线项，不给静默默认）。
func WithMutex(m Mutex) CronOption { return func(o *cronOptions) { o.mutex = m } }

// WithLedger 注入外置运行台账（DB 落库/审计流等），与内置 MemLedger 并联
// 记录；nil = 只用内置（显式回退默认值，不算非法）。
func WithLedger(l Ledger) CronOption { return func(o *cronOptions) { o.ledger = l } }

// WithInstance 设置实例标识（台账归因/排障），缺省取主机名。
func WithInstance(id string) CronOption { return func(o *cronOptions) { o.instance = id } }

// WithClock 注入时钟（触发点计算/租约到期/台账时刻同源），nil 取
// local.SystemClock{}；测试注入 local.FakeClock 确定性驱动（与
// local.Scheduler 的 Clock 注入同一形态，同一假时钟两层共用）。
func WithClock(clock local.Clock) CronOption { return func(o *cronOptions) { o.clock = clock } }

// WithLogger 注入日志器，nil 取 slog.Default()（根模块纯 stdlib，与
// local.Scheduler 同口径）。
func WithLogger(l *slog.Logger) CronOption { return func(o *cronOptions) { o.logger = l } }

// WithRetryBudget 设置执行重试预算（互斥锁 TTL 预算公式的唯一出处，
// 见 RetryBudget）。
func WithRetryBudget(b RetryBudget) CronOption { return func(o *cronOptions) { o.budget = b } }

// WithStaleSweep 设置启动清扫钩子（见 StaleSweepFunc）。
func WithStaleSweep(fn StaleSweepFunc) CronOption { return func(o *cronOptions) { o.staleSweep = fn } }

// WithRunCapacity 设置内置运行台账容量（有界环形，<=0 取 DefaultRunCapacity）。
func WithRunCapacity(n int) CronOption { return func(o *cronOptions) { o.runCapacity = n } }

// WithConcurrency 设置全局任务并发度（透传 local.Config.Concurrency，
// <=0 取 1 严格串行）。
func WithConcurrency(n int) CronOption { return func(o *cronOptions) { o.concurrency = n } }

// WithCatchUp 设置错过触发点的补偿策略（透传 local.CatchUpPolicy）。
func WithCatchUp(p local.CatchUpPolicy) CronOption { return func(o *cronOptions) { o.catchUp = p } }

// WithLocation 设置 cron 表达式时区（透传 local.Config.Location，nil 取
// time.Local）。
func WithLocation(loc *time.Location) CronOption { return func(o *cronOptions) { o.location = loc } }

// planState 是已装入触发引擎的计划状态（指纹 diff 的本地镜像）。
type planState struct {
	spec        string
	fingerprint string // 归一后的指纹（Plan.Fingerprint 为空时派生 = Spec）
}

// planFingerprint 返回计划的 diff 指纹（留空派生 = Spec，见 Plan.Fingerprint）。
func planFingerprint(p Plan) string {
	if p.Fingerprint != "" {
		return p.Fingerprint
	}
	return p.Spec
}

// Scheduler 是多实例动态 cron 装配面（#159）：local.Scheduler 触发引擎 +
// 指纹 diff 增量装配 + 触发互斥 + 运行台账。零值不可用，New 构造。
type Scheduler struct {
	exec  Executor
	cfg   cronOptions
	log   *slog.Logger
	inner *local.Scheduler
	mem   *MemLedger

	// slotMu 进程内 slot 互斥（租约型，内建常开）：摘旧装新的在飞旧触发
	// 与新条目补跑可撞同一 slot，进程内先裁决，不劳分布式锁。
	slotMu *LocalMutex

	mu      sync.Mutex
	plans   map[string]*planState
	stopped bool

	engineCtx       context.Context
	engineCancel    context.CancelFunc
	engineDone      chan struct{}
	engineStarted   atomic.Bool
	startEngineOnce func() // sync.OnceFunc：触发引擎只拉起一次
	stopOnce        func() // sync.OnceFunc：停机收口（Close/ctx 取消同路）
	stopCh          chan struct{}
}

// New 创建调度器。构造期 fail-fast（#97 同哲学）：nil Executor、非法
// CatchUp 等配置错误在启动期炸响，不带病运行。
func New(exec Executor, opts ...CronOption) *Scheduler {
	if exec == nil {
		panic("cron.New: nil Executor")
	}
	cfg := cronOptions{
		// 默认预算对齐根包延迟任务量级（见 budget.go 常量注释）。
		budget: RetryBudget{
			Attempts: DefaultBudgetAttempts,
			Timeout:  DefaultBudgetTimeout,
			Backoff:  DefaultBudgetBackoff,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.clock == nil {
		cfg.clock = local.SystemClock{}
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.runCapacity <= 0 {
		cfg.runCapacity = DefaultRunCapacity
	}
	if cfg.instance == "" {
		if host, err := os.Hostname(); err == nil {
			cfg.instance = host
		}
	}
	inner, err := local.New(local.Config{
		Concurrency: cfg.concurrency,
		CatchUp:     cfg.catchUp,
		Clock:       cfg.clock,
		Logger:      cfg.logger,
		Location:    cfg.location,
	})
	if err != nil {
		panic("cron.New: " + err.Error())
	}

	s := &Scheduler{
		exec:       exec,
		cfg:        cfg,
		log:        cfg.logger,
		inner:      inner,
		mem:        NewMemLedger(cfg.runCapacity),
		slotMu:     NewLocalMutex(cfg.clock),
		plans:      make(map[string]*planState),
		engineDone: make(chan struct{}),
		stopCh:     make(chan struct{}),
	}
	s.engineCtx, s.engineCancel = context.WithCancel(context.Background())
	s.startEngineOnce = sync.OnceFunc(func() {
		s.engineStarted.Store(true)
		gosafe.Go(func() {
			defer close(s.engineDone)
			// Start 返回值按 local.Start 语义（#99）：停机正常路径也带
			// ctx.Err()——只对意外错误留痕。
			if err := s.inner.Start(s.engineCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Error("trigger engine stopped with error", timertask.ErrKey, err)
			}
		})
	})
	s.stopOnce = sync.OnceFunc(func() {
		close(s.stopCh)
		s.engineCancel()
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
	})
	return s
}

// Reconcile 把期望计划集收敛到触发引擎（#159「指纹 diff 增量重装」）：
// 期望集里的启用条目按指纹增量 upsert（指纹未变幂等空操作、变化摘旧装新），
// 停用条目与已装但不在期望集里的条目注销。多实例共用同一 DB 期望集、
// 周期对账的场景下，期望集未变时 Reconcile 是**空操作**（不重装、不重置
// @every 触发基准）。
//
// 对账语义（与 local.Reconcile 同骨架）：
//
//   - 启停/变更由期望集表达：启用的条目出现在期望集里（Enabled=true），
//     停用的不出现或 Enabled=false（同效注销）；
//   - 单条失败（坏 spec 等）**不阻断**其余条目收敛，失败逐条汇总进返回
//     错误（errors.Join）；
//   - 期望集内 ID 重复 / 空 ID ⇒ 「以哪条为准」存在歧义，fail-closed：
//     本次调用不做任何变更并返回错误；
//   - 首次调用自动拉起触发引擎（StartReconcile 形态亦然）。
//
// errors 全部成功时返回 nil；调用方通常应串行化对账（单 goroutine 周期
// 触发）——并发调用无数据竞争，但收敛次序不保证。停机后返回
// ErrSchedulerStopped（实例一次性，#124）。
func (s *Scheduler) Reconcile(ctx context.Context, plans []Plan) error {
	seen := make(map[string]bool, len(plans))
	for i := range plans {
		if plans[i].ID == "" {
			return errors.New("cron: reconcile: empty plan ID in desired set")
		}
		if seen[plans[i].ID] {
			return fmt.Errorf("cron: reconcile: duplicate plan ID %q in desired set", plans[i].ID)
		}
		seen[plans[i].ID] = true
	}

	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return ErrSchedulerStopped
	}
	s.startEngineOnce()

	var errs []error
	for i := range plans {
		p := plans[i]
		if !p.Enabled {
			s.Remove(ctx, p.ID) // 停用 = 注销（与「不在期望集」同效）
			continue
		}
		if err := s.install(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("cron: reconcile %q: %w", p.ID, err))
		}
	}
	// 已装但不在期望集：注销（期望集是唯一事实源）。
	s.mu.Lock()
	vanished := make([]string, 0, len(s.plans))
	for id := range s.plans {
		if !seen[id] {
			vanished = append(vanished, id)
		}
	}
	s.mu.Unlock()
	for _, id := range vanished {
		s.Remove(ctx, id)
	}
	return errors.Join(errs...)
}

// Upsert 按单计划 upsert（运行期增/改）：指纹未变幂等空操作、变化摘旧
// 装新；Enabled=false 等价 Remove。不触碰其它已装计划（与 Reconcile 的
// 期望集收敛相对——单条管理面，供管理 API/事件驱动改单条）。停机后返回
// ErrSchedulerStopped。
func (s *Scheduler) Upsert(ctx context.Context, p Plan) error {
	if p.ID == "" {
		return errors.New("cron: upsert: empty plan ID")
	}
	if !p.Enabled {
		s.Remove(ctx, p.ID)
		return nil
	}
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return ErrSchedulerStopped
	}
	s.startEngineOnce()
	return s.install(ctx, p)
}

// Remove 按计划 ID 注销（运行期删），返回是否确实移除了触发条目。正在
// 执行的触发不打断（执行已进台账），未开始的触发丢弃（local.UnscheduleFor
// 同语义）。
func (s *Scheduler) Remove(_ context.Context, id string) bool {
	s.mu.Lock()
	delete(s.plans, id)
	s.mu.Unlock()
	return s.inner.UnscheduleFor(id)
}

// StartReconcile 启动「启动清扫 + 周期对账 + 触发引擎」并阻塞（#159
// `StartReconcile`；返回 error 为生命周期纪律，退出语义与 local.Start
// 同款——#99）：
//
//  1. 启动清扫（WithStaleSweep）先于首次对账——zeus/task 的
//     FailStaleRunning 启动清扫 + 清扫后补发事件同序，补发在钩子内完成；
//  2. 立即 listFn() → Reconcile 首轮对账，随后按 interval 周期收敛
//     （listFn 失败/单条收敛失败只记日志不中断循环——多实例对账要韧性）；
//  3. ctx 取消 ⇒ 停机（触发引擎同停）返回 ctx.Err()；Close 触发的正常
//     停机返回 nil。
//
// 周期等待走注入时钟（clock.NewTimer 可停 timer，#174）——FakeClock 测试下
// 对账节奏确定性可控，不真睡。自带循环的调用方直接周期调 Reconcile 即可。
func (s *Scheduler) StartReconcile(ctx context.Context, interval time.Duration, listFn func() ([]Plan, error)) error {
	if interval <= 0 {
		return errors.New("cron: StartReconcile: interval must be > 0")
	}
	if listFn == nil {
		return errors.New("cron: StartReconcile: nil listFn")
	}
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return ErrSchedulerStopped
	}

	if s.cfg.staleSweep != nil {
		swept, err := s.cfg.staleSweep(ctx)
		if err != nil {
			s.log.Warn("stale sweep failed", "swept", swept, timertask.ErrKey, err)
		} else {
			s.log.Info("stale sweep done", "swept", swept)
		}
	}
	s.startEngineOnce()

	for {
		s.reconcileOnce(ctx, listFn)
		// #174：可停 timer——旧实现 clock.After 每轮新建不可停 timer，ctx
		// 取消 / Close 退出时遗弃一个挂满整个 interval 的 timer；醒来/退出
		// 即 Stop。
		timer := s.cfg.clock.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			s.stopOnce()
			return ctx.Err()
		case <-s.stopCh:
			timer.Stop()
			return nil // Close 触发的正常停机（#99 同口径）
		case <-timer.C():
		}
	}
}

// Close 停止调度并等待在飞触发收尾（组件生命周期统一 Close(ctx) error +
// sync.Once 幂等）：对账循环与触发引擎同停，正在执行的任务不打断（等待到
// ctx 截止，local.Stop 同语义）。ctx 超时返回 ctx.Err()，任务继续跑完。
// 可重复调用；未启动过触发引擎时立即返回。
func (s *Scheduler) Close(ctx context.Context) error {
	s.stopOnce()
	if err := s.inner.Stop(ctx); err != nil {
		return err
	}
	if s.engineStarted.Load() {
		select {
		case <-s.engineDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// RunHistory 返回内置台账记录（时间序旧→新）；planID 非空时只返回该计划的。
func (s *Scheduler) RunHistory(planID string) []RunRecord {
	if planID == "" {
		return s.mem.Recent()
	}
	return s.mem.RecentFor(planID)
}

// install 是单条计划的增量装配（Reconcile/Upsert 共用）：指纹 diff 判定 +
// local.ScheduleFor 装配。指纹未变零调用（不重置触发基准）；指纹变化且
// spec 未变时先摘后装强制重置（local.ScheduleFor 按 spec 判幂等会空操作）。
func (s *Scheduler) install(_ context.Context, p Plan) error {
	fp := planFingerprint(p)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrSchedulerStopped
	}
	cur := s.plans[p.ID]
	if cur != nil && cur.fingerprint == fp {
		s.mu.Unlock()
		return nil // 指纹未变：幂等空操作（防 @every 被周期重装重置触发点）
	}
	forced := cur != nil && cur.spec == p.Spec
	s.mu.Unlock()

	fn := func(ctx context.Context, at time.Time) error { return s.fire(ctx, p.ID, at) }
	if forced {
		s.inner.UnscheduleFor(p.ID)
	}
	if err := s.inner.ScheduleFor(p.ID, p.Spec, fn); err != nil {
		if forced {
			// 摘了没装上：状态与实际一致（未装），下轮对账重试。
			s.mu.Lock()
			delete(s.plans, p.ID)
			s.mu.Unlock()
		}
		return fmt.Errorf("install %q: %w", p.ID, err)
	}
	s.mu.Lock()
	s.plans[p.ID] = &planState{spec: p.Spec, fingerprint: fp}
	s.mu.Unlock()
	return nil
}

// reconcileOnce 跑一轮对账（listFn 失败/收敛失败只记日志——周期循环的
// 韧性面，不中断）。
func (s *Scheduler) reconcileOnce(ctx context.Context, listFn func() ([]Plan, error)) {
	plans, err := listFn()
	if err != nil {
		s.log.Error("list plans failed, will retry next tick", timertask.ErrKey, err)
		return
	}
	if err := s.Reconcile(ctx, plans); err != nil {
		s.log.Warn("reconcile partial failure", timertask.ErrKey, err)
	}
}

// fire 是一次触发的完整执行面：slot 互斥（进程内租约 trylock + 可选跨实例
// Mutex）→ 执行（panic 收编）→ 运行台账。
//
// 互斥语义（slot 认领，见 Mutex 类型注释）：触发点的租约不随执行完成释放，
// 认领标记保留到 TTL 到期——同触发点的迟到重复触发一律让位（触发点恰好
// 一次）；执行超过预算（TTL 过期）后被他实例接管是预算公式的边界，见
// RetryBudget。
//
// 返回值语义：仅「真的执行了且失败」返回错误（local 层记 task failed）；
// 互斥让位/互斥设施故障是**未执行**（不是任务失败），记台账 + 日志后返回
// nil——让位是多实例常态（Debug），设施故障是告警面（Error）。
func (s *Scheduler) fire(ctx context.Context, planID string, at time.Time) error {
	return s.fireAttempt(ctx, planID, at, 0)
}

// fireAttempt 带 attempt 计数的触发执行面（#239 接管重试的防环闸）：
// attempt==0 是正常调度路径；==1 是崩溃接管重试（只重试一次）。
func (s *Scheduler) fireAttempt(ctx context.Context, planID string, at time.Time, attempt int) error {
	trigger := at.UTC().Format(time.RFC3339)
	key := execLockKey(planID, trigger)
	ttl := s.cfg.budget.LockTTL() // 锁 TTL 预算公式单一出处（RetryBudget）

	// ① 进程内 slot trylock（租约型，zeus「触发秒 slot 分布式锁+进程内
	//    trylock」的进程内层）。
	innerOwner, err := s.slotMu.TryLock(ctx, key, ttl)
	if errors.Is(err, ErrLockNotAcquired) {
		s.recordSkip(ctx, planID, trigger, nil)
		return nil
	}
	if err != nil {
		s.recordSkip(ctx, planID, trigger, err)
		s.log.Error("exec slot lock failed, trigger skipped", "plan", planID, "trigger", trigger, timertask.ErrKey, err)
		return nil
	}
	if innerOwner == "" {
		// 防御：契约要求成功必回非空 ownerID，空值按未获得处置（fail-closed）。
		s.recordSkip(ctx, planID, trigger, nil)
		return nil
	}

	// ② 跨实例互斥（可选）：distlock 等经 Mutex 缝注入，未注入 = 单实例契约。
	if s.cfg.mutex != nil {
		owner, merr := s.cfg.mutex.TryLock(ctx, key, ttl)
		switch {
		case errors.Is(merr, ErrLockNotAcquired):
			// 同触发点已由他实例认领/执行：让位，台账可观测（跨实例恰好
			// 一次的正常形态）。
			s.recordSkip(ctx, planID, trigger, nil)
			s.log.Debug("exec slot taken by other instance", "plan", planID, "trigger", trigger)
			// #239：接管重试（一次性）——「TTL 到期自动可被他实例接管」的
			// 接管者此前不存在：认领即消费，唯一一次 fire 落空后该触发点
			// 永久丢失（持有者崩溃形态）。TTL 到期后重试一次；重试前查台账
			// 防双跑（原持有者正常完成 ⇒ 让位；仍被持 ⇒ 持有者活着且执行
			// 超预算，RetryBudget 边界 ⇒ 放弃）。
			if attempt == 0 {
				s.scheduleTakeoverRetry(ctx, planID, at, ttl)
			}
			return nil
		case merr != nil:
			// 互斥设施故障：fail-closed 跳过（宁可漏跑一轮可补发，不可双跑）。
			s.recordSkip(ctx, planID, trigger, merr)
			s.log.Error("exec mutex failed, trigger skipped", "plan", planID, "trigger", trigger, timertask.ErrKey, merr)
			return nil
		case owner == "":
			// 防御：同上，空 ownerID 按未获得处置。
			s.recordSkip(ctx, planID, trigger, nil)
			return nil
		}
	}

	// ③ 执行 + 台账（成功/失败/panic 收编都留痕）。认领租约不提前释放
	//    （slot 认领语义，见函数注释）。
	rec := RunRecord{
		PlanID:    planID,
		Trigger:   trigger,
		Instance:  s.cfg.instance,
		StartedAt: s.cfg.clock.Now(),
	}
	err = s.execSafe(ctx, planID, trigger)
	rec.EndedAt = s.cfg.clock.Now()
	rec.Err = err
	s.record(ctx, rec)
	return err
}

// execSafe 调用执行函数并把 panic 收编为错误（panic 不得打死触发引擎，
// 且收编结果照常进台账——根包 runHandlerSafe 同款纪律）。panic 收编单源
// （pkg errors.Recovered，#171）：身份与栈由 PanicError 携带。
func (s *Scheduler) execSafe(ctx context.Context, planID, trigger string) (err error) {
	defer func() {
		if perr := pkgerrors.Recovered(recover()); perr != nil {
			err = fmt.Errorf("cron: executor panic: %w", perr)
		}
	}()
	return s.exec(ctx, planID, trigger)
}

// takeoverRetryGuard 是接管重试在租约 TTL 之上的等待余量：租约到期语义
// （TTL 后可接管）与重试时刻之间留出可见性缓冲。
const takeoverRetryGuard = 2 * time.Second

// scheduleTakeoverRetry 排一次 TTL 后的接管重试（#239，一次性，不递归）。
// 重试前查运行台账：本触发点已有非 skip 记录（原持有者正常完成）⇒ 不重跑
// ——租约「保留到 TTL 到期」的防重设计对崩溃与正常完成一视同仁，盲重试
// 会把已完成的触发双跑。无法核验（外部台账只写不查）⇒ 保守放弃接管
// （宁可不接管，不可双跑）。
func (s *Scheduler) scheduleTakeoverRetry(ctx context.Context, planID string, at time.Time, ttl time.Duration) {
	gosafe.Go(func() {
		t := time.NewTimer(ttl + takeoverRetryGuard)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
		if s.triggerAlreadyRan(planID, at.UTC().Format(time.RFC3339)) {
			return
		}
		_ = s.fireAttempt(ctx, planID, at, 1)
	})
}

// triggerAlreadyRan 查内置 MemLedger 是否已有本触发点的**执行**记录
// （非 skip）。外部 Ledger 只写不查 ⇒ 无法核验时保守返回 true（不重跑）。
func (s *Scheduler) triggerAlreadyRan(planID, trigger string) bool {
	for _, rec := range s.mem.RecentFor(planID) {
		if rec.Trigger == trigger && !rec.Skipped {
			return true
		}
	}
	return false
}

// recordSkip 记一条「未执行」台账（让位/故障 fail-closed，见 RunRecord）。
func (s *Scheduler) recordSkip(ctx context.Context, planID, trigger string, cause error) {
	now := s.cfg.clock.Now()
	s.record(ctx, RunRecord{
		PlanID:    planID,
		Trigger:   trigger,
		Instance:  s.cfg.instance,
		StartedAt: now,
		EndedAt:   now,
		Err:       cause,
		Skipped:   true,
	})
}

// record 落运行台账（内置 MemLedger + 外置 Ledger 并联），panic 收编——
// 台账故障不得打死触发。panic 收编单源（pkg errors.Recovered，#171）：
// 台账 panic 的类型/身份/栈由 PanicError 携带。
func (s *Scheduler) record(ctx context.Context, rec RunRecord) {
	defer func() {
		if perr := pkgerrors.Recovered(recover()); perr != nil {
			s.log.Error("run ledger panic recovered", "panic", perr)
		}
	}()
	s.mem.Record(ctx, rec)
	if s.cfg.ledger != nil {
		s.cfg.ledger.Record(ctx, rec)
	}
}
