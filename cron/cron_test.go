package cron

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/timertask/local"
)

// 测试基准时刻：2026-11-11T00:00:00Z（FakeClock 驱动，触发点串可精确断言）。
func testEpoch() time.Time {
	return time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
}

// execProbe 记录执行调用（planID@trigger）到信道，测试可精确断言触发串。
type execProbe struct {
	ch     chan string
	panics bool // 首次调用 panic（收编契约测试用）
	mu     sync.Mutex
	n      int
}

func newExecProbe() *execProbe { return &execProbe{ch: make(chan string, 16)} }

func (p *execProbe) exec(_ context.Context, planID, trigger string) error {
	p.mu.Lock()
	p.n++
	n := p.n
	p.mu.Unlock()
	p.ch <- planID + "@" + trigger
	if p.panics && n == 1 {
		panic("boom")
	}
	return nil
}

// chanLedger 把台账记录旁路进信道（跨实例/外置台账观测用）。
type chanLedger struct{ ch chan<- RunRecord }

func (l chanLedger) Record(_ context.Context, rec RunRecord) { l.ch <- rec }

// stubMutex 是 Mutex 的脚注实现：TryLock 恒返回预设结果。
type stubMutex struct {
	tryErr error // 非 nil ⇒ TryLock 返回该错误
}

func (m *stubMutex) TryLock(_ context.Context, _ string, _ time.Duration) (string, error) {
	if m.tryErr != nil {
		return "", m.tryErr
	}
	return "stub-owner", nil
}

func (m *stubMutex) Unlock(_ context.Context, _ string, _ string) error { return nil }

// newTestScheduler 构造假时钟驱动的调度器，测试退出自动 Close。
func newTestScheduler(t *testing.T, exec Executor, opts ...CronOption) (*Scheduler, *local.FakeClock) {
	t.Helper()
	clock := local.NewFakeClock(testEpoch())
	s := New(exec, append([]CronOption{WithClock(clock), WithInstance("test")}, opts...)...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s, clock
}

// waitCalls 从执行信道收 n 个调用（超时即失败）。
func waitCalls(t *testing.T, ch <-chan string, n int) []string {
	t.Helper()
	var out []string
	deadline := time.After(3 * time.Second)
	for len(out) < n {
		select {
		case got := <-ch:
			out = append(out, got)
		case <-deadline:
			t.Fatalf("timeout: 等 %d 次执行, got %v", n, out)
		}
	}
	return out
}

// waitRecords 等台账积累到 n 条（超时即失败）。
func waitRecords(t *testing.T, s *Scheduler, n int) []RunRecord {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		recs := s.RunHistory("")
		if len(recs) >= n {
			return recs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: 等 %d 条台账, got %d", n, len(recs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestNew_NilExecutorPanics 构造期 fail-fast（#97 同哲学）。
func TestNew_NilExecutorPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(nil) 应构造期 panic")
		}
	}()
	_ = New(nil)
}

// TestReconcile_FiresAndTriggerString 装配 + 触发：trigger 为触发点
// RFC3339（秒精度），与互斥键/台账键同源。
func TestReconcile_FiresAndTriggerString(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "t1@2026-11-11T00:01:00Z" {
		t.Fatalf("触发串不符: %q", got[0])
	}
	recs := waitRecords(t, s, 1)
	if recs[0].PlanID != "t1" || recs[0].Trigger != "2026-11-11T00:01:00Z" ||
		recs[0].Skipped || recs[0].Err != nil || recs[0].Instance != "test" {
		t.Fatalf("台账记录不符: %+v", recs[0])
	}
	if got := s.RunHistory("t1"); len(got) != 1 {
		t.Fatalf("RunHistory(t1) 应 1 条, got %d", len(got))
	}
}

// TestReconcile_UnchangedFingerprintKeepsTriggerBase 指纹未变的周期对账是
// **幂等空操作**（#159 核心防呆）：不重装、不重置 @every 触发基准——
// 此前「周期性重装」会把间隔长于重装周期的任务永不触发。
func TestReconcile_UnchangedFingerprintKeepsTriggerBase(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	plans := []Plan{{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true}}
	if err := s.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(30 * time.Second) // t0+30s：周期对账到（指纹未变）
	if err := s.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("Reconcile(幂等): %v", err)
	}
	clock.Advance(30 * time.Second) // t0+1m：基准未重置 ⇒ 该触发
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "t1@2026-11-11T00:01:00Z" {
		t.Fatalf("指纹未变不得重置触发基准: %q", got[0])
	}
}

// TestReconcile_FingerprintChangeReinstalls 指纹变化 ⇒ 摘旧装新：计划被
// 编辑过，从新基准起算（zeus taskFingerprint 语义）。
func TestReconcile_FingerprintChangeReinstalls(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(30 * time.Second) // t0+30s：指纹变化（同 spec）重装
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Fingerprint: "fp2", Enabled: true},
	}); err != nil {
		t.Fatalf("Reconcile(指纹变化): %v", err)
	}
	clock.Advance(30 * time.Second) // t0+1m：基准已重置到 t0+30s ⇒ 此刻不触发
	select {
	case got := <-probe.ch:
		t.Fatalf("重装后基准=t0+30s，t0+1m 不该触发: %q", got)
	case <-time.After(200 * time.Millisecond):
	}
	clock.Advance(30 * time.Second) // t0+1m30s：新基准的首个触发点
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "t1@2026-11-11T00:01:30Z" {
		t.Fatalf("重装后的触发点不符: %q", got[0])
	}
}

// TestReconcile_DisableAndVanishUninstall 停用（Enabled=false）与消失
// （不在期望集）同效注销：不再触发。
func TestReconcile_DisableAndVanishUninstall(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Enabled: true},
		{ID: "t2", Spec: "@every 1m", Enabled: true},
		{ID: "t3", Spec: "@every 1m", Enabled: true},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// t2 停用（Enabled=false）、t3 消失（不在期望集）——同效注销。
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Enabled: true},
		{ID: "t2", Spec: "@every 1m", Enabled: false},
	}); err != nil {
		t.Fatalf("Reconcile(停用): %v", err)
	}
	clock.Advance(time.Minute)
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "t1@2026-11-11T00:01:00Z" {
		t.Fatalf("只有 t1 该触发: %q", got[0])
	}
	select {
	case got := <-probe.ch:
		t.Fatalf("停用/消失的计划不得触发: %q", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestReconcile_DuplicateIDFailClosed 期望集 ID 重复 ⇒「以哪条为准」歧义，
// 本次调用零变更 fail-closed（与 local.Reconcile 同口径）。
func TestReconcile_DuplicateIDFailClosed(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Enabled: true},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	err := s.Reconcile(context.Background(), []Plan{
		{ID: "t1", Spec: "@every 1m", Enabled: true},
		{ID: "t1", Spec: "@every 2m", Enabled: true},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("重复 ID 应 fail-closed 报错, got %v", err)
	}
	// 零变更：t1 仍是原 @every 1m（既未被换成 2m 也未被注销）。
	clock.Advance(time.Minute)
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "t1@2026-11-11T00:01:00Z" {
		t.Fatalf("fail-closed 不得改动既有装配: %q", got[0])
	}
}

// TestReconcile_EmptyIDFailClosed 空 ID 同样整体 fail-closed。
func TestReconcile_EmptyIDFailClosed(t *testing.T) {
	probe := newExecProbe()
	s, _ := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{{Spec: "@every 1m", Enabled: true}}); err == nil {
		t.Fatal("空 ID 应报错")
	}
}

// TestReconcile_BadSpecAggregated 单条坏 spec 不阻断其余收敛，失败逐条
// 汇总（errors.Join——多条错误一次给出，不逐个挤牙膏）。
func TestReconcile_BadSpecAggregated(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	err := s.Reconcile(context.Background(), []Plan{
		{ID: "good", Spec: "@every 1m", Enabled: true},
		{ID: "bad1", Spec: "not a cron", Enabled: true},
		{ID: "bad2", Spec: "0 0 31 2 *", Enabled: true}, // 永无触发点（ErrUnsatisfiableSpec）
	})
	if err == nil {
		t.Fatal("坏 spec 应报错")
	}
	if !strings.Contains(err.Error(), "bad1") || !strings.Contains(err.Error(), "bad2") {
		t.Fatalf("errors.Join 应一次汇总全部坏条目: %v", err)
	}
	// good 不受阻断，照常触发。
	clock.Advance(time.Minute)
	got := waitCalls(t, probe.ch, 1)
	if got[0] != "good@2026-11-11T00:01:00Z" {
		t.Fatalf("好条目不得被坏条目阻断: %q", got[0])
	}
}

// TestUpsertAndRemove 单条管理面（运行期增/改/删）：指纹未变幂等、
// Enabled=false 等价 Remove、Remove 注销。
func TestUpsertAndRemove(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	ctx := context.Background()

	if err := s.Upsert(ctx, Plan{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// 指纹未变：幂等空操作（同 Reconcile 口径）。
	if err := s.Upsert(ctx, Plan{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true}); err != nil {
		t.Fatalf("Upsert(幂等): %v", err)
	}
	// Upsert 不触碰其它计划（与 Reconcile 期望集收敛相对）。
	if err := s.Upsert(ctx, Plan{ID: "t2", Spec: "@every 1m", Enabled: true}); err != nil {
		t.Fatalf("Upsert(t2): %v", err)
	}
	clock.Advance(time.Minute)
	got := waitCalls(t, probe.ch, 2)
	if len(got) != 2 {
		t.Fatalf("t1/t2 都该触发: %v", got)
	}

	// Enabled=false 等价 Remove。
	if err := s.Upsert(ctx, Plan{ID: "t2", Spec: "@every 1m", Enabled: false}); err != nil {
		t.Fatalf("Upsert(停用): %v", err)
	}
	if !s.Remove(ctx, "t1") {
		t.Fatal("Remove(t1) 应返回 true")
	}
	if s.Remove(ctx, "t1") {
		t.Fatal("重复 Remove 应返回 false")
	}
}

// TestFire_LedgerFanOut 外置台账与内置 MemLedger 并联（运行台账可观测）。
func TestFire_LedgerFanOut(t *testing.T) {
	probe := newExecProbe()
	ext := make(chan RunRecord, 4)
	s, clock := newTestScheduler(t, probe.exec, WithLedger(chanLedger{ch: ext}))
	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	waitCalls(t, probe.ch, 1)
	select {
	case rec := <-ext:
		if rec.PlanID != "t1" || rec.Skipped {
			t.Fatalf("外置台账记录不符: %+v", rec)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("外置台账未收到记录")
	}
	if recs := waitRecords(t, s, 1); len(recs) != 1 {
		t.Fatalf("内置台账应 1 条, got %d", len(recs))
	}
}

// TestFire_ExecutorPanicRecorded 执行 panic 收编为失败记录（不打死触发
// 引擎，根包 runHandlerSafe 同款纪律）。
func TestFire_ExecutorPanicRecorded(t *testing.T) {
	probe := newExecProbe()
	probe.panics = true
	s, clock := newTestScheduler(t, probe.exec)
	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	waitCalls(t, probe.ch, 1)
	recs := waitRecords(t, s, 1)
	if recs[0].Skipped || recs[0].Err == nil || !strings.Contains(recs[0].Err.Error(), "panic") {
		t.Fatalf("panic 应收编为失败记录: %+v", recs[0])
	}
	// 触发引擎存活：下一轮照常（probe.ch 已消费首条，这里只等新增的一条）。
	clock.Advance(time.Minute)
	waitCalls(t, probe.ch, 1)
}

// TestFire_MutexContentionSkip 跨实例互斥让位：ErrLockNotAcquired ⇒ 未执行
// 记台账（Skipped，Err=nil），不是任务失败。
func TestFire_MutexContentionSkip(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec, WithMutex(&stubMutex{tryErr: ErrLockNotAcquired}))
	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	recs := waitRecords(t, s, 1)
	if !recs[0].Skipped || recs[0].Err != nil {
		t.Fatalf("让位记录应 Skipped 且无错: %+v", recs[0])
	}
	select {
	case got := <-probe.ch:
		t.Fatalf("让位不得执行: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestFire_MutexInfraFailureSkip 互斥设施故障 ⇒ fail-closed 跳过（宁可漏跑
// 一轮可补发，不可双跑），台账记错可观测。
func TestFire_MutexInfraFailureSkip(t *testing.T) {
	probe := newExecProbe()
	infraErr := errors.New("redis down")
	s, clock := newTestScheduler(t, probe.exec, WithMutex(&stubMutex{tryErr: infraErr}))
	if err := s.Reconcile(context.Background(), []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	recs := waitRecords(t, s, 1)
	if !recs[0].Skipped || !errors.Is(recs[0].Err, infraErr) {
		t.Fatalf("设施故障应 Skipped+Err 留痕: %+v", recs[0])
	}
	select {
	case got := <-probe.ch:
		t.Fatalf("设施故障不得执行: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMultiInstance_MutexArbitrates 互斥并发场景（#159 核心）：两个实例
// 共享互斥、同一触发点恰好一次执行——赢家执行记账、让位方记 Skipped。
func TestMultiInstance_MutexArbitrates(t *testing.T) {
	clock := local.NewFakeClock(testEpoch())
	shared := NewLocalMutex(clock) // 跨实例互斥的共享实现（distlock 测试替身）
	recCh := make(chan RunRecord, 8)
	var execCount atomic.Int64
	exec := func(_ context.Context, _, _ string) error {
		execCount.Add(1)
		return nil
	}

	newInst := func(name string) *Scheduler {
		return New(exec,
			WithClock(clock),
			WithMutex(shared),
			WithLedger(chanLedger{ch: recCh}),
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

	plans := []Plan{{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true}}
	if err := s1.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("s1 Reconcile: %v", err)
	}
	if err := s2.Reconcile(context.Background(), plans); err != nil {
		t.Fatalf("s2 Reconcile: %v", err)
	}
	clock.Advance(time.Minute) // 两实例同刻触发同一 slot

	var recs []RunRecord
	deadline := time.After(3 * time.Second)
	for len(recs) < 2 {
		select {
		case rec := <-recCh:
			recs = append(recs, rec)
		case <-deadline:
			t.Fatalf("timeout: 两实例各记一条, got %+v", recs)
		}
	}
	if got := execCount.Load(); got != 1 {
		t.Fatalf("同一触发点应恰好一次执行, got %d", got)
	}
	var executed, skipped int
	for _, rec := range recs {
		if rec.Trigger != "2026-11-11T00:01:00Z" {
			t.Fatalf("两实例应同触发点（互斥键同源）: %+v", rec)
		}
		if rec.Skipped {
			skipped++
		} else {
			executed++
		}
	}
	if executed != 1 || skipped != 1 {
		t.Fatalf("恰好一次执行 + 一次让位, got executed=%d skipped=%d", executed, skipped)
	}
	if recs[0].Instance == recs[1].Instance {
		t.Fatalf("台账应可归因到不同实例: %+v", recs)
	}
}

// TestStartReconcile_SweepBeforeFirstList 启动清扫先于首次对账且只跑一次
// （zeus FailStaleRunning 启动清扫同序）。
func TestStartReconcile_SweepBeforeFirstList(t *testing.T) {
	probe := newExecProbe()
	var mu sync.Mutex
	var order []string
	sweep := func(_ context.Context) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "sweep")
		return 3, nil
	}
	listFn := func() ([]Plan, error) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "list")
		return nil, nil
	}
	s, clock := newTestScheduler(t, probe.exec, WithStaleSweep(sweep))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.StartReconcile(ctx, time.Minute, listFn) }()

	// 等首轮（sweep+list）完成后补两个 tick，确认 sweep 不重复。
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("首轮未完成: order=%v", order)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for range 2 {
		clock.Advance(time.Minute)
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 || order[0] != "sweep" || order[1] != "list" {
		t.Fatalf("清扫必须先于首次对账: order=%v", order)
	}
	sweeps := 0
	for _, ev := range order {
		if ev == "sweep" {
			sweeps++
		}
	}
	if sweeps != 1 {
		t.Fatalf("启动清扫只跑一次, got %d", sweeps)
	}
	_ = errCh
}

// TestStartReconcile_PeriodicConvergence 周期对账收敛：listFn 期望集变化
// 被增量装配（新计划随后触发）。
func TestStartReconcile_PeriodicConvergence(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	var calls atomic.Int64
	listFn := func() ([]Plan, error) {
		n := calls.Add(1)
		if n == 1 {
			return []Plan{{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true}}, nil
		}
		return []Plan{
			{ID: "t1", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true},
			{ID: "t2", Spec: "@every 1m", Fingerprint: "fp1", Enabled: true},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.StartReconcile(ctx, time.Minute, listFn) }()

	// 推进假时钟直到第二轮对账发生（补推容忍调度协程注册时钟等待的时差）。
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("第二轮对账未发生, calls=%d", calls.Load())
		}
		clock.Advance(time.Minute)
		time.Sleep(10 * time.Millisecond)
	}
	clock.Advance(time.Minute) // t2 装配后的触发点
	deadlineCh := time.After(3 * time.Second)
	sawT2 := false
	for !sawT2 {
		select {
		case got := <-probe.ch:
			if strings.HasPrefix(got, "t2@") {
				sawT2 = true
			}
		case <-deadlineCh:
			t.Fatal("周期对账新装的 t2 未触发")
		}
	}
}

// TestStartReconcile_ListErrorResilient listFn 失败只记日志不中断循环
// （多实例对账要韧性，下一轮重试）。
func TestStartReconcile_ListErrorResilient(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	var calls atomic.Int64
	listFn := func() ([]Plan, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("db down")
		}
		return []Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.StartReconcile(ctx, time.Minute, listFn) }()

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("listFn 失败后循环应继续")
		}
		clock.Advance(time.Minute)
		time.Sleep(10 * time.Millisecond)
	}
	clock.Advance(time.Minute)
	waitCalls(t, probe.ch, 1) // 第二轮装配的计划照常触发
}

// TestStartReconcile_CancelReturnsCtxErr ctx 取消 ⇒ 停机返回 ctx.Err()，
// 实例进入终态（#124 实例一次性）。
func TestStartReconcile_CancelReturnsCtxErr(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	_ = clock
	started := make(chan struct{}, 1)
	listFn := func() ([]Plan, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.StartReconcile(ctx, time.Minute, listFn) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("StartReconcile 未启动")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 取消应返回 ctx.Err(), got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StartReconcile 未退出")
	}
	if err := s.Reconcile(context.Background(), nil); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("停机后应 ErrSchedulerStopped, got %v", err)
	}
}

// TestStartReconcile_CloseReturnsNil Close 触发的正常停机返回 nil（#99
// 同口径：正常停机不得被伪装成失败）。
func TestStartReconcile_CloseReturnsNil(t *testing.T) {
	probe := newExecProbe()
	s, clock := newTestScheduler(t, probe.exec)
	_ = clock
	started := make(chan struct{}, 1)
	listFn := func() ([]Plan, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		return nil, nil
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.StartReconcile(context.Background(), time.Minute, listFn) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("StartReconcile 未启动")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Close 触发的正常停机应返回 nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StartReconcile 未退出")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close 幂等重复调用应 nil, got %v", err)
	}
}

// TestStartReconcile_Validation 参数校验（构造期 fail-fast 的运行期对应面）。
func TestStartReconcile_Validation(t *testing.T) {
	probe := newExecProbe()
	s, _ := newTestScheduler(t, probe.exec)
	if err := s.StartReconcile(context.Background(), 0, func() ([]Plan, error) { return nil, nil }); err == nil {
		t.Fatal("interval<=0 应报错")
	}
	if err := s.StartReconcile(context.Background(), time.Minute, nil); err == nil {
		t.Fatal("nil listFn 应报错")
	}
}

// TestUpsertAfterStopTerminal 停机终态（#124 实例一次性）：Upsert/Reconcile/
// StartReconcile 一律显式报错，不静默哑火。
func TestUpsertAfterStopTerminal(t *testing.T) {
	probe := newExecProbe()
	s, _ := newTestScheduler(t, probe.exec)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Upsert(context.Background(), Plan{ID: "t1", Spec: "@every 1m", Enabled: true}); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("停机后 Upsert 应报错, got %v", err)
	}
	if err := s.StartReconcile(context.Background(), time.Minute, func() ([]Plan, error) { return nil, nil }); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("停机后 StartReconcile 应报错, got %v", err)
	}
}
