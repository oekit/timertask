package local

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// testStart 是测试用的固定起点（UTC，整秒对齐）。
var testStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// waitFor 等待条件成立（最多 timeout）。用于等待内部状态变化，
// 不用于等待「触发点到达」——触发点由假时钟驱动，完全确定性。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// newTestScheduler 创建带假时钟的调度器（Location 固定 UTC，避免本机时区影响）。
func newTestScheduler(t *testing.T, cfg Config) (*Scheduler, *FakeClock) {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = NewFakeClock(testStart)
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, cfg.Clock.(*FakeClock)
}

// start 启动调度器并注册清理（测试结束自动停止）。
func start(t *testing.T, s *Scheduler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		_ = s.Stop(stopCtx)
	})
	return cancel
}

// TestScheduler_TriggerSequence_FakeClock 钉住触发点序列：假时钟推进，
// 触发点按 cron 表达式依次到达，不依赖 sleep。
func TestScheduler_TriggerSequence_FakeClock(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	fired := make(chan time.Time, 16)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	for i := 1; i <= 3; i++ {
		clock.Advance(5 * time.Second)
		select {
		case at := <-fired:
			want := testStart.Add(time.Duration(i*5) * time.Second)
			if !at.Equal(want) {
				t.Errorf("step %d: 触发时间 = %v，期望 %v", i, at, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("step %d: 2s 内未触发", i)
		}
	}
}

// TestScheduler_CatchUpSkip 钉住默认策略：错过的触发点只执行最近的一个。
// 时钟一次跳过两个触发点（5s、10s），只应执行 10s 那次。
func TestScheduler_CatchUpSkip(t *testing.T) {
	s, clock := newTestScheduler(t, Config{CatchUp: CatchUpSkip})

	fired := make(chan time.Time, 16)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(10 * time.Second)
	select {
	case at := <-fired:
		want := testStart.Add(10 * time.Second)
		if !at.Equal(want) {
			t.Errorf("触发时间 = %v，期望 %v（最近的触发点）", at, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内未触发")
	}

	// 再推进一个周期：下一次触发应为 15s（证明 5s 已被跳过，而非补跑）
	clock.Advance(5 * time.Second)
	select {
	case at := <-fired:
		want := testStart.Add(15 * time.Second)
		if !at.Equal(want) {
			t.Errorf("触发时间 = %v，期望 %v", at, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内未触发")
	}
}

// TestScheduler_CatchUpAll 钉住补跑策略：错过的触发点按时间顺序逐一补跑。
func TestScheduler_CatchUpAll(t *testing.T) {
	s, clock := newTestScheduler(t, Config{CatchUp: CatchUpAll})

	fired := make(chan time.Time, 16)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(10 * time.Second)
	for i, want := range []time.Time{
		testStart.Add(5 * time.Second),
		testStart.Add(10 * time.Second),
	} {
		select {
		case at := <-fired:
			if !at.Equal(want) {
				t.Errorf("补跑 %d: 触发时间 = %v，期望 %v", i, at, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("补跑 %d: 2s 内未触发", i)
		}
	}
}

// TestScheduler_SameSpecSerial 钉住「同一 spec 串行」：上一轮没跑完时，
// 新触发进入该 spec 的待执行队列，不并发执行。
func TestScheduler_SameSpecSerial(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	release := make(chan struct{})
	started := make(chan time.Time, 4)
	var runs atomic.Int32
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		runs.Add(1)
		started <- at
		<-release // 第一次执行阻塞
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(5 * time.Second)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("首次触发未到达")
	}

	// 第二个触发点到达：此时上一轮仍在执行
	clock.Advance(5 * time.Second)
	if !waitFor(t, 2*time.Second, func() bool {
		return s.pendingCount() == 1
	}) {
		t.Fatal("第二个触发未进入待执行队列")
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("并发执行了：runs = %d，期望 1（同一 spec 串行）", got)
	}

	// 放行后，第二轮才开始
	close(release)
	select {
	case at := <-started:
		want := testStart.Add(10 * time.Second)
		if !at.Equal(want) {
			t.Errorf("第二轮触发时间 = %v，期望 %v", at, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("第二轮未执行")
	}
}

// TestScheduler_ConcurrencyAcrossSpecs 钉住并发度：不同 spec 之间按
// Config.Concurrency 并发，同一 spec 仍串行。
func TestScheduler_ConcurrencyAcrossSpecs(t *testing.T) {
	s, clock := newTestScheduler(t, Config{Concurrency: 2})

	release := make(chan struct{})
	started := make(chan string, 8)
	block := func(name string) Task {
		return func(_ context.Context, _ time.Time) error {
			started <- name
			<-release
			return nil
		}
	}
	if err := s.Schedule("*/5 * * * * *", block("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule("*/10 * * * * *", block("b")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(10 * time.Second)
	// 两个 spec 都应启动（并发度 2）
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("只启动了 %d 个任务（并发度 2 应启动 2 个）", i)
		}
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("启动的任务 = %v，期望 a 与 b", seen)
	}
	close(release)
}

// TestScheduler_StopWaitsForRunningTask 钉住退出语义：Stop 不打断正在执行的
// 任务；等待超时返回 ctx.Err()，任务跑完后 Stop 返回 nil。
func TestScheduler_StopWaitsForRunningTask(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, _ time.Time) error {
		started <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(5 * time.Second)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("任务未启动")
	}

	// Stop 等待超时：任务仍在跑 → 返回 ctx.Err()
	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Stop(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop = %v，期望 DeadlineExceeded（任务仍在执行）", err)
	}

	// 任务跑完后再次 Stop：应正常返回
	close(release)
	okCtx, okCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer okCancel()
	if err := s.Stop(okCtx); err != nil {
		t.Errorf("Stop = %v，期望 nil", err)
	}
}

// TestScheduler_TaskErrorDoesNotKillScheduler 钉住：任务返回错误只记日志，
// 调度器继续工作（下一个触发点仍然到达）。
func TestScheduler_TaskErrorDoesNotKillScheduler(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	fired := make(chan time.Time, 4)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		return errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	for i := 1; i <= 2; i++ {
		clock.Advance(5 * time.Second)
		select {
		case <-fired:
		case <-time.After(2 * time.Second):
			t.Fatalf("第 %d 次触发未到达（任务错误不应中断调度）", i)
		}
	}
}

// TestScheduler_TaskPanicDoesNotKillScheduler 钉住：任务 panic 被收编，
// 不打死 runner goroutine（后续触发点仍然到达）。
func TestScheduler_TaskPanicDoesNotKillScheduler(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	fired := make(chan time.Time, 4)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		panic("task boom")
	}); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	for i := 1; i <= 2; i++ {
		clock.Advance(5 * time.Second)
		select {
		case <-fired:
		case <-time.After(2 * time.Second):
			t.Fatalf("第 %d 次触发未到达（panic 不应中断调度）", i)
		}
	}
}

// TestScheduler_DuplicateSpec 钉住：同一 spec 重复注册报错（不静默覆盖）。
func TestScheduler_DuplicateSpec(t *testing.T) {
	s, _ := newTestScheduler(t, Config{})
	task := func(context.Context, time.Time) error { return nil }

	if err := s.Schedule("*/5 * * * * *", task); err != nil {
		t.Fatal(err)
	}
	err := s.Schedule("*/5 * * * * *", task)
	if !errors.Is(err, ErrDuplicateSpec) {
		t.Errorf("重复注册 = %v，期望 ErrDuplicateSpec", err)
	}
}

// TestScheduler_InvalidSpec 钉住：非法 cron 表达式在注册时报错。
func TestScheduler_InvalidSpec(t *testing.T) {
	s, _ := newTestScheduler(t, Config{})
	if err := s.Schedule("not-a-cron", func(context.Context, time.Time) error { return nil }); err == nil {
		t.Error("非法 cron 表达式应返回错误")
	}
	if err := s.Schedule("*/5 * * * * *", nil); err == nil {
		t.Error("nil 任务应返回错误")
	}
}

// TestScheduler_Unschedule 钉住：注销后不再触发。
func TestScheduler_Unschedule(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	fired := make(chan time.Time, 4)
	if err := s.Schedule("*/5 * * * * *", func(_ context.Context, at time.Time) error {
		fired <- at
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !s.Unschedule("*/5 * * * * *") {
		t.Error("Unschedule 应返回 true")
	}
	if s.Unschedule("*/5 * * * * *") {
		t.Error("重复 Unschedule 应返回 false")
	}
	start(t, s)

	clock.Advance(10 * time.Second)
	select {
	case at := <-fired:
		t.Errorf("注销后仍触发了：%v", at)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestScheduler_StartTwice 钉住：重复 Start 返回错误。
func TestScheduler_StartTwice(t *testing.T) {
	s, _ := newTestScheduler(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	if !waitFor(t, 2*time.Second, func() bool { return s.isStarted() }) {
		t.Fatal("Start 未生效")
	}
	if err := s.Start(ctx); err == nil {
		t.Error("重复 Start 应返回错误")
	}
}

// TestScheduler_InvalidConfig 钉住：非法 catch-up 策略在 New 时报错。
func TestScheduler_InvalidConfig(t *testing.T) {
	if _, err := New(Config{CatchUp: CatchUpPolicy(42)}); err == nil {
		t.Error("非法 catch-up 策略应返回错误")
	}
}

// TestFakeClock 钉住假时钟自身行为：After 到期才唤醒，Advance 推进时间。
func TestFakeClock(t *testing.T) {
	c := NewFakeClock(testStart)
	if got := c.Now(); !got.Equal(testStart) {
		t.Errorf("Now = %v，期望 %v", got, testStart)
	}

	ch := c.After(5 * time.Second)
	select {
	case <-ch:
		t.Error("未到期不应唤醒")
	default:
	}

	c.Advance(5 * time.Second)
	select {
	case at := <-ch:
		if !at.Equal(testStart.Add(5 * time.Second)) {
			t.Errorf("唤醒时间 = %v，期望 %v", at, testStart.Add(5*time.Second))
		}
	case <-time.After(time.Second):
		t.Error("到期后应唤醒")
	}

	// d <= 0 立即就绪
	ready := c.After(0)
	select {
	case <-ready:
	default:
		t.Error("d <= 0 应立即就绪")
	}
}
