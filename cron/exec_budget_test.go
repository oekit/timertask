package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newBudgetScheduler 构造真实时钟的执行预算测试调度器（执行超时/退避走
// 真实时间，测试用毫秒级预算；锁租约仍走同一预算公式）。
func newBudgetScheduler(t *testing.T, exec Executor, budget RetryBudget) *Scheduler {
	t.Helper()
	s := New(exec, WithRetryBudget(budget), WithInstance("test"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// TestFire_ExecutorTimeoutBudget 钉住 #292：RetryBudget.Timeout 真正约束每次
// 执行（context.WithTimeout），Attempts 驱动重试。此前 fireAttempt 直接用
// 引擎 ctx——无时限、无重试，Timeout/Attempts/Backoff 只参与 LockTTL 公式，
// Executor 跑超 LockTTL 即租约到期可被他实例接管（双跑）。
func TestFire_ExecutorTimeoutBudget(t *testing.T) {
	var calls atomic.Int32
	exec := func(ctx context.Context, _, _ string) error {
		calls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	budget := RetryBudget{Attempts: 2, Timeout: 20 * time.Millisecond, Backoff: 5 * time.Millisecond, Slack: time.Second}
	s := newBudgetScheduler(t, exec, budget)

	start := time.Now()
	err := s.fire(context.Background(), "p1", testEpoch().Add(time.Minute))
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fire = %v, want context.DeadlineExceeded（Timeout 未约束执行）", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("executor 调用 %d 次, want 2（Attempts 未驱动重试）", got)
	}
	// 两次 20ms 超时 + 一次 5ms 退避：下界留 40ms 防调度抖动。
	if elapsed < 40*time.Millisecond {
		t.Fatalf("执行耗时 %v, want >= 40ms（每次尝试受 Timeout 约束）", elapsed)
	}
	recs := s.RunHistory("")
	if len(recs) != 1 || recs[0].Skipped || !errors.Is(recs[0].Err, context.DeadlineExceeded) {
		t.Fatalf("台账应记录一次非 skip 的超时失败: %+v", recs)
	}
}

// TestFire_ExecutorBudgetRetriesUntilSuccess 失败后按 Attempts/Backoff 重试，
// 成功即停——成功的结果只记一条 Err=nil 台账，不把中间失败当终态。
func TestFire_ExecutorBudgetRetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	transient := errors.New("transient")
	exec := func(context.Context, string, string) error {
		if calls.Add(1) == 1 {
			return transient
		}
		return nil
	}
	budget := RetryBudget{Attempts: 3, Timeout: time.Second, Backoff: time.Millisecond, Slack: time.Second}
	s := newBudgetScheduler(t, exec, budget)

	if err := s.fire(context.Background(), "p1", testEpoch().Add(time.Minute)); err != nil {
		t.Fatalf("第二次尝试成功后 fire 应返回 nil, got %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("executor 调用 %d 次, want 2（成功即停）", got)
	}
	recs := s.RunHistory("")
	if len(recs) != 1 || recs[0].Skipped || recs[0].Err != nil {
		t.Fatalf("台账应为一次成功记录: %+v", recs)
	}
}

// TestFire_ExecutorBudgetAttemptsOneNoRetry Attempts=1 表示只尝试一次
// （零值归一后的下界），失败不重试、直接进台账。
func TestFire_ExecutorBudgetAttemptsOneNoRetry(t *testing.T) {
	var calls atomic.Int32
	boom := errors.New("boom")
	exec := func(context.Context, string, string) error {
		calls.Add(1)
		return boom
	}
	budget := RetryBudget{Attempts: 1, Timeout: time.Second, Slack: time.Second}
	s := newBudgetScheduler(t, exec, budget)

	if err := s.fire(context.Background(), "p1", testEpoch().Add(time.Minute)); !errors.Is(err, boom) {
		t.Fatalf("fire = %v, want boom", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor 调用 %d 次, want 1（Attempts=1 不重试）", got)
	}
}
