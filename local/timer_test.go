package local

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// countingClock 统计 NewTimer/Stop 调用（#174 可停 timer 回归的观测面）：
// NewTimer 计数 > 0 证明循环等待改走可停 timer 形态；每次重算/退出都有配对
// Stop 证明旧 timer 不再被遗弃。
type countingClock struct {
	Clock
	newTimers atomic.Int32
	stops     atomic.Int32
}

func (c *countingClock) NewTimer(d time.Duration) Timer {
	c.newTimers.Add(1)
	return &countingTimer{inner: c.Clock.NewTimer(d), clock: c}
}

type countingTimer struct {
	inner Timer
	clock *countingClock
}

func (t *countingTimer) C() <-chan time.Time { return t.inner.C() }

func (t *countingTimer) Stop() bool {
	t.clock.stops.Add(1)
	return t.inner.Stop()
}

// TestFakeClock_NewTimer_Stop 钉住 FakeClock 可停 timer 的契约（#174）：
// Stop 摘除等待者并保证 C 永不再发信号；已触发/已停止后 Stop 返回 false；
// d <= 0 立即就绪。
func TestFakeClock_NewTimer_Stop(t *testing.T) {
	c := NewFakeClock(testStart)

	tm := c.NewTimer(5 * time.Second)
	select {
	case <-tm.C():
		t.Error("未到期不应唤醒")
	default:
	}
	if !tm.Stop() {
		t.Error("未触发的 timer Stop 应返回 true")
	}
	c.Advance(5 * time.Second)
	select {
	case at := <-tm.C():
		t.Errorf("Stop 后不应再收到信号, got %v", at)
	default:
	}
	if tm.Stop() {
		t.Error("已停止的 timer Stop 应返回 false")
	}

	// 到期后 Stop 返回 false
	tm2 := c.NewTimer(time.Second)
	c.Advance(time.Second)
	select {
	case <-tm2.C():
	default:
		t.Fatal("到期后应收到信号")
	}
	if tm2.Stop() {
		t.Error("已触发的 timer Stop 应返回 false")
	}

	// d <= 0 立即就绪
	tm3 := c.NewTimer(0)
	select {
	case <-tm3.C():
	default:
		t.Error("d <= 0 应立即就绪")
	}
	if tm3.Stop() {
		t.Error("立即就绪的 timer Stop 应返回 false（已触发）")
	}
}

// TestScheduler_Wake_StopsAbandonedTimer 钉住 #174：调度循环被 wake（注册/
// 注销任务触发重算）打断时必须 Stop 本轮 timer——旧实现 clock.After 每轮新建
// 不可停 timer，wake 时旧 timer 遗弃在原到期点，堆积量 = 注册频率 × 剩余等待
// （daily cron 场景 wait 可达小时级）。
func TestScheduler_Wake_StopsAbandonedTimer(t *testing.T) {
	base := NewFakeClock(testStart)
	cc := &countingClock{Clock: base}
	s, err := New(Config{Clock: cc})
	if err != nil {
		t.Fatal(err)
	}
	nop := func(context.Context, time.Time) error { return nil }

	// 先注册一个条目：无条目时 run 循环等待 wake、不注册 timer
	if err := s.Schedule("@every 1h", nop); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Start(ctx) }()

	// 等 run 循环注册第一轮等待
	if !waitFor(t, 2*time.Second, func() bool { return cc.newTimers.Load() >= 1 }) {
		t.Fatal("run 循环未注册等待 timer")
	}

	// 两次 wake：每次重算都必须 Stop 旧 timer 再注册新 timer
	if err := s.Schedule("@every 2h", nop); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule("@every 3h", nop); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return cc.stops.Load() >= 2 }) {
		t.Fatalf("wake 重算未停止旧 timer（#174）: newTimers=%d stops=%d",
			cc.newTimers.Load(), cc.stops.Load())
	}

	// 假时钟的等待队列不堆积：遗留等待者不超过当前这轮的 1 个
	// （白盒：同包测试可直接读 waiters； Stop 已把旧等待者摘出队列）
	base.mu.Lock()
	n := len(base.waiters)
	base.mu.Unlock()
	if n > 1 {
		t.Fatalf("假时钟等待者堆积 = %d, want <= 1（#174）", n)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 后 Start 未返回")
	}
}
