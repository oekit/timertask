package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/timertask/local"
)

// takeoverRetryTTL 是接管重试测试的锁 TTL 预算：Attempts=1、Timeout=1s、
// Slack=1s ⇒ LockTTL = 2s，接管重试时刻 = TTL + takeoverRetryGuard = 4s
// （@every 1m 的下一触发点在 60s 外，测试内的推进不串到下一档）。
var takeoverRetryTTL = RetryBudget{Attempts: 1, Timeout: time.Second, Slack: time.Second}.LockTTL()

// probingClock 记录每次 NewTimer 的请求时长（#259「接管重试走注入时钟」的
// 观测面）：调度引擎的周期等待是 60s 级，唯一请求 ttl+guard 时长的只有
// 接管重试 timer——按时长匹配可精准钉住该路径，不被引擎自身 timer 噪声
// 干扰。
type probingClock struct {
	local.Clock
	mu   sync.Mutex
	durs []time.Duration
}

func (c *probingClock) NewTimer(d time.Duration) local.Timer {
	c.mu.Lock()
	c.durs = append(c.durs, d)
	c.mu.Unlock()
	return c.Clock.NewTimer(d)
}

func (c *probingClock) requested(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, got := range c.durs {
		if got == d {
			return true
		}
	}
	return false
}

func (c *probingClock) count(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, got := range c.durs {
		if got == d {
			n++
		}
	}
	return n
}

// contendOnceMutex 首次 TryLock 让位（ErrLockNotAcquired），其后放行——
// 模拟「他实例持有租约，TTL 到期后本实例接管」的 #239 接管场景。
type contendOnceMutex struct {
	contended atomic.Bool
}

func (m *contendOnceMutex) TryLock(_ context.Context, _ string, _ time.Duration) (string, error) {
	if m.contended.CompareAndSwap(false, true) {
		return "", ErrLockNotAcquired
	}
	return "takeover-owner", nil
}

func (m *contendOnceMutex) Unlock(_ context.Context, _ string, _ string) error { return nil }

// queryLedger 是外置台账的可查询测试替身（#292）：实现 LedgerQuery，
// 按 (planID, trigger) 报告是否已有非 skip 执行记录（线程安全，可多实例共享）。
type queryLedger struct {
	mu       sync.Mutex
	recs     []RunRecord
	queryErr error
}

func (l *queryLedger) Record(_ context.Context, rec RunRecord) {
	l.mu.Lock()
	l.recs = append(l.recs, rec)
	l.mu.Unlock()
}

func (l *queryLedger) Ran(_ context.Context, planID, trigger string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.queryErr != nil {
		return false, l.queryErr
	}
	for _, rec := range l.recs {
		if rec.PlanID == planID && rec.Trigger == trigger && !rec.Skipped {
			return true, nil
		}
	}
	return false, nil
}

func (l *queryLedger) records() []RunRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]RunRecord(nil), l.recs...)
}

// newTakeoverScheduler 构造接管重试测试的假时钟调度器：probingClock 观测
// NewTimer 请求时长（#259 注入时钟的断言面），测试退出自动 Close。
//
// 缺省注入一个空的 queryLedger（#292）：接管重试要求台账可核验，只写不查
// 的台账会让重试保守放弃——接管路径的测试需要可查询面；测试可用
// WithLedger 覆盖成共享台账或只写台账。
func newTakeoverScheduler(t *testing.T, exec Executor, m Mutex, opts ...CronOption) (*Scheduler, *local.FakeClock, *probingClock) {
	t.Helper()
	base := local.NewFakeClock(testEpoch())
	pc := &probingClock{Clock: base}
	baseOpts := make([]CronOption, 0, 5+len(opts))
	baseOpts = append(baseOpts,
		WithClock(pc),
		WithMutex(m),
		WithRetryBudget(RetryBudget{Attempts: 1, Timeout: time.Second, Slack: time.Second}),
		WithInstance("test"),
		WithLedger(&queryLedger{}),
	)
	s := New(exec, append(baseOpts, opts...)...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s, base, pc
}

// waitRetryArmed 等接管重试 timer 在注入时钟上完成注册（skip 台账先于
// gosafe.Go 里的 NewTimer 落地——不先等注册就推进假时钟，timer 会注册在
// 已推过的时刻上永不触发，测试与异步触发处理竞态）。
func waitRetryArmed(t *testing.T, pc *probingClock) {
	t.Helper()
	if !waitForCond(2*time.Second, func() bool {
		return pc.requested(takeoverRetryTTL + takeoverRetryGuard)
	}) {
		t.Fatalf("接管重试未走 clock.NewTimer(ttl+guard)（#259 注入时钟）")
	}
}

// TestFire_TakeoverRetry_FiresAfterTTLGuard 接管重试确定性驱动（#239 +
// #259）：首次认领让位 ⇒ 记 skip 台账 + 排 ttl+guard 后的重试 timer；假时钟
// 推进到 TTL 不触发（重试时刻在 guard 之后），推过 guard ⇒ 恰好重试一次并
// 留执行台账。#259：重试等待必须走注入时钟（clock.NewTimer）——旧实现裸
// time.NewTimer 下 FakeClock 推进不生效，只能真实时间硬等 4s。
func TestFire_TakeoverRetry_FiresAfterTTLGuard(t *testing.T) {
	probe := newExecProbe()
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &contendOnceMutex{})

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute) // 触发 00:01:00：slot 认领成功，跨实例互斥让位

	recs := waitRecords(t, s, 1)
	if !recs[0].Skipped || recs[0].Err != nil {
		t.Fatalf("让位记录应 Skipped 且无错: %+v", recs[0])
	}
	waitRetryArmed(t, pc) // #259：重试等待走注入时钟，时长恰为 ttl+guard

	// 推进到 TTL：guard 未过，重试不得提前触发。
	clock.Advance(takeoverRetryTTL)
	select {
	case got := <-probe.ch:
		t.Fatalf("TTL 到但 guard 未过，不得提前接管: %q", got)
	case <-time.After(100 * time.Millisecond):
	}

	// 推过 guard：重试生效——slot 租约（TTL 到期）已可接管，本实例执行。
	clock.Advance(takeoverRetryGuard)
	calls := waitCalls(t, probe.ch, 1)
	if calls[0] != "t1@2026-11-11T00:01:00Z" {
		t.Fatalf("接管重试应执行原触发点, got %q", calls[0])
	}
	recs = waitRecords(t, s, 2)
	if recs[1].Skipped {
		t.Fatalf("重试执行应留非 skip 台账: %+v", recs[1])
	}
	if recs[1].Trigger != recs[0].Trigger {
		t.Fatalf("重试应针对原触发点: %+v vs %+v", recs[1], recs[0])
	}
}

// TestFire_TakeoverRetry_AlreadyRanNoRerun 接管重试的防双跑闸：重试前查
// 运行台账，本触发点已有非 skip 记录（原持有者正常完成）⇒ 放弃重试——
// 租约防重设计对崩溃与正常完成一视同仁。
func TestFire_TakeoverRetry_AlreadyRanNoRerun(t *testing.T) {
	probe := newExecProbe()
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &contendOnceMutex{})

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	if recs := waitRecords(t, s, 1); !recs[0].Skipped {
		t.Fatalf("让位记录应 Skipped: %+v", recs[0])
	}
	waitRetryArmed(t, pc)

	// 原持有者「正常完成」：重试 timer 到期前补一条非 skip 台账（他实例
	// 只写不查的台账形态——triggerAlreadyRan 只核内置 MemLedger）。
	now := clock.Now()
	s.mem.Record(context.Background(), RunRecord{
		PlanID:    "t1",
		Trigger:   "2026-11-11T00:01:00Z",
		Instance:  "other",
		StartedAt: now,
		EndedAt:   now,
	})
	before := len(s.RunHistory(""))

	clock.Advance(takeoverRetryTTL + takeoverRetryGuard)
	select {
	case got := <-probe.ch:
		t.Fatalf("已完成的触发不得重跑: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
	if got := len(s.RunHistory("")); got != before {
		t.Fatalf("放弃重试不得新增台账, got %d -> %d", before, got)
	}
}

// TestFire_TakeoverRetry_OnceOnly 接管重试一次性（#239，不递归）：重试仍
// 让位 ⇒ 记 skip 后不再排下一轮重试（attempt==1 不再进 scheduleTakeoverRetry）
// ——接管失败退回「该触发点本档丢失」，由下一轮 @every 或外部补发兜底。
func TestFire_TakeoverRetry_OnceOnly(t *testing.T) {
	probe := newExecProbe()
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &stubMutex{tryErr: ErrLockNotAcquired}) // 恒让位

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute) // 首次：让位 + 排重试
	waitRetryArmed(t, pc)
	clock.Advance(takeoverRetryTTL + takeoverRetryGuard) // 重试：仍让位，不再递归

	recs := waitRecords(t, s, 2)
	for _, rec := range recs {
		if !rec.Skipped || rec.Trigger != "2026-11-11T00:01:00Z" {
			t.Fatalf("两次都应是同一触发点的让位记录: %+v", recs)
		}
	}

	// 递归重试（若存在）会在 skip 台账 #2 之后随即注册第二个 ttl+guard
	// timer（fireAttempt 内 recordSkip 先于 scheduleTakeoverRetry）——留出
	// 注册窗口后断言请求次数仍为 1，钉死一次性（不递归）。
	waitForCond(200*time.Millisecond, func() bool {
		return pc.count(takeoverRetryTTL+takeoverRetryGuard) >= 2
	})
	if got := pc.count(takeoverRetryTTL + takeoverRetryGuard); got != 1 {
		t.Fatalf("接管重试必须一次性（#239 不递归）, ttl+guard timer 注册 %d 次", got)
	}

	// 再推进远超一轮重试周期（仍在下一触发点 00:02:00 之前）：无第三次
	// 台账、无执行。
	clock.Advance(10 * time.Second)
	select {
	case got := <-probe.ch:
		t.Fatalf("接管重试不得递归: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
	if got := len(s.RunHistory("")); got != 2 {
		t.Fatalf("重试后不得再排接管（一次性）, got %d 条台账", got)
	}
}

// TestFire_TakeoverRetry_UnverifiableLedgerAbandons 钉住 #292 的保守面：
// 外置台账只写不查（未实现 LedgerQuery）⇒ 本实例无记录时无法核验他实例是否
// 已执行，接管重试必须放弃（宁可不接管，不可双跑）；旧实现无条件按「未执行」
// 处理，TTL 后重跑已完成的触发点。
func TestFire_TakeoverRetry_UnverifiableLedgerAbandons(t *testing.T) {
	probe := newExecProbe()
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &contendOnceMutex{},
		WithLedger(chanLedger{ch: make(chan RunRecord, 8)})) // 只写不查

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	if recs := waitRecords(t, s, 1); !recs[0].Skipped {
		t.Fatalf("让位记录应 Skipped: %+v", recs[0])
	}
	waitRetryArmed(t, pc)

	clock.Advance(takeoverRetryTTL + takeoverRetryGuard)
	select {
	case got := <-probe.ch:
		t.Fatalf("台账不可核验时不得接管重跑: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
	if got := len(s.RunHistory("")); got != 1 {
		t.Fatalf("放弃接管不得新增台账, got %d", got)
	}
}

// TestFire_TakeoverRetry_QueryErrorAbandons 台账查询失败同样按无法核验处理
// （#292）：宁可漏接管一轮（可补发），不可双跑。
func TestFire_TakeoverRetry_QueryErrorAbandons(t *testing.T) {
	probe := newExecProbe()
	led := &queryLedger{queryErr: errors.New("ledger db down")}
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &contendOnceMutex{}, WithLedger(led))

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	waitRecords(t, s, 1)
	waitRetryArmed(t, pc)

	clock.Advance(takeoverRetryTTL + takeoverRetryGuard)
	select {
	case got := <-probe.ch:
		t.Fatalf("台账查询失败时不得接管重跑: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestFire_TakeoverRetry_ExternalLedgerAlreadyRanNoRerun #292 的核验面：
// 执行发生在**他实例**（本实例 MemLedger 只有自己的 skip 记录），可查询的
// 外置台账报告该触发点已执行 ⇒ 接管重试放弃，不双跑。
func TestFire_TakeoverRetry_ExternalLedgerAlreadyRanNoRerun(t *testing.T) {
	probe := newExecProbe()
	led := &queryLedger{}
	now := testEpoch().Add(time.Minute)
	led.Record(context.Background(), RunRecord{
		PlanID:    "t1",
		Trigger:   "2026-11-11T00:01:00Z",
		Instance:  "other",
		StartedAt: now,
		EndedAt:   now,
	})
	s, clock, pc := newTakeoverScheduler(t, probe.exec, &contendOnceMutex{}, WithLedger(led))

	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	waitRecords(t, s, 1) // 本实例只有 skip 台账（他实例的执行不在 MemLedger）
	waitRetryArmed(t, pc)

	clock.Advance(takeoverRetryTTL + takeoverRetryGuard)
	select {
	case got := <-probe.ch:
		t.Fatalf("外置台账已报告执行，不得重跑: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMultiInstance_TakeoverRetry_SharedLedgerPreventsDoubleRun 双实例回归
// （#292）：A 持有跨实例锁并完成执行，B 让位后按 TTL+guard 排接管重试。
// B 的本进程 MemLedger 没有 A 的记录（旧实现据此判定「未执行」⇒ 双跑）；
// 共享的可查询台账报告该触发点已执行 ⇒ B 放弃接管，全场景恰好一次执行。
func TestMultiInstance_TakeoverRetry_SharedLedgerPreventsDoubleRun(t *testing.T) {
	clock := local.NewFakeClock(testEpoch())
	sharedMutex := NewLocalMutex(clock)
	led := &queryLedger{}
	var execCount atomic.Int64
	exec := func(_ context.Context, _, _ string) error {
		execCount.Add(1)
		return nil
	}
	budget := RetryBudget{Attempts: 1, Timeout: time.Second, Slack: time.Second}
	newInst := func(name string) *Scheduler {
		return New(exec,
			WithClock(clock),
			WithMutex(sharedMutex),
			WithLedger(led),
			WithRetryBudget(budget),
			WithInstance(name),
		)
	}
	s1, s2 := newInst("i1"), newInst("i2")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s1.Close(ctx)
		_ = s2.Close(ctx)
	})

	plans := []Plan{{ID: "takeover", Spec: "@every 10m", Fingerprint: "fp1", Enabled: true}}
	if err := s1.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("s1 Reconcile: %v", err)
	}
	if err := s2.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("s2 Reconcile: %v", err)
	}
	clock.Advance(10 * time.Minute) // 两实例同刻触发同一 slot

	if !waitForCond(3*time.Second, func() bool { return len(led.records()) >= 2 }) {
		t.Fatalf("双实例台账未落地: %+v", led.records())
	}
	recs := led.records()
	var executed, skipped int
	for _, rec := range recs {
		if rec.Skipped {
			skipped++
		} else {
			executed++
		}
	}
	if executed != 1 || skipped != 1 {
		t.Fatalf("恰好一次执行 + 一次让位, got executed=%d skipped=%d (%+v)", executed, skipped, recs)
	}
	if got := execCount.Load(); got != 1 {
		t.Fatalf("首触发应恰好一次执行, got %d", got)
	}

	// 让位方在 TTL+guard 后的接管重试：共享台账已有 A 的执行记录 ⇒ 放弃。
	clock.Advance(takeoverRetryTTL + takeoverRetryGuard)
	time.Sleep(100 * time.Millisecond)
	if got := execCount.Load(); got != 1 {
		t.Fatalf("接管重试双跑：execCount = %d, want 1（#292 台账核验失效）", got)
	}
	if got := len(led.records()); got != 2 {
		t.Fatalf("放弃接管不得新增台账, got %d 条", got)
	}
}
