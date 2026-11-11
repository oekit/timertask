package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/timertask/local"
)

// countingClock 统计 NewTimer/Stop 调用（#174 可停 timer 回归的观测面）。
type countingClock struct {
	local.Clock
	newTimers atomic.Int32
	stops     atomic.Int32
}

func (c *countingClock) NewTimer(d time.Duration) local.Timer {
	c.newTimers.Add(1)
	return &countingTimer{inner: c.Clock.NewTimer(d), clock: c}
}

type countingTimer struct {
	inner local.Timer
	clock *countingClock
}

func (t *countingTimer) C() <-chan time.Time { return t.inner.C() }

func (t *countingTimer) Stop() bool {
	t.clock.stops.Add(1)
	return t.inner.Stop()
}

// TestStartReconcile_StopsTimerOnExit 钉住 #174：对账循环的周期等待必须走
// 可停 timer（clock.NewTimer）而非 clock.After——旧实现每轮新建不可停 timer，
// ctx 取消 / Close 退出时遗弃一个挂满整个 interval 的 timer；且退出路径必须
// 配对 Stop。
func TestStartReconcile_StopsTimerOnExit(t *testing.T) {
	probe := newExecProbe()
	base := local.NewFakeClock(testEpoch())
	cc := &countingClock{Clock: base}
	s := New(probe.exec, WithClock(cc), WithInstance("test"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})

	listFn := func() ([]Plan, error) { return nil, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.StartReconcile(ctx, time.Minute, listFn) }()

	// 等首轮对账完成并注册周期等待 timer
	if !waitForCond(2*time.Second, func() bool { return cc.newTimers.Load() >= 1 }) {
		t.Fatalf("对账循环未走 clock.NewTimer（#174 可停 timer）: newTimers=%d", cc.newTimers.Load())
	}

	// Close 退出：等待中的 timer 必须被 Stop（不遗弃）
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := s.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if cc.stops.Load() < 1 {
		t.Fatalf("退出路径未 Stop 周期 timer（#174）: stops=%d", cc.stops.Load())
	}
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后 StartReconcile 未返回")
	}
}

// waitForCond 轮询等待条件成立（cron 包内的独立版本，避免依赖 cron_test.go
// 的 helper 布局）。
func waitForCond(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
