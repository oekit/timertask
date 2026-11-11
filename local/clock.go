package local

import (
	"sync"
	"time"

	"github.com/oekit/timertask"
)

// Clock 是时钟抽象，便于注入假时钟做确定性测试。
//
// 调度器的所有时间判断都走本接口：注册时的基准时间、触发点计算、等待下一个
// 触发点。注入 FakeClock 后，整个调度过程不需要 sleep 即可断言触发序列。
// Clock 是时钟抽象（别名归一到根包 timertask.Clock——单一事实源，#78）。
// 本调度器还需要 After 与 NewTimer（等待下一个触发点），根包接口不含这两个
// 方法 ⇒ 保留本包扩展接口，嵌入根包定义。
//
// NewTimer（#174）是循环等待场景的必用形态：After 每次调用都遗弃一个不可停
// timer——调度循环被 wake（注册/注销任务）打断重算时，旧 timer 全部挂到原
// 到期点堆积（堆积量 = 打断频率 × 剩余等待）；NewTimer 返回可停面，醒来即停。
type Clock interface {
	timertask.Clock
	// After 返回一个通道：在 d 之后收到一个时间信号。
	// d <= 0 时必须返回「已就绪」的通道（调用方据此立即重算，不空等）。
	// 注意：After 形态的 timer 不可停，循环等待 + 可被唤醒打断的场景请用
	// NewTimer（#174）。
	After(d time.Duration) <-chan time.Time
	// NewTimer 返回可停 timer（#174）：C 通道在 d 之后收到信号（d <= 0 时
	// 立即就绪），Stop 停止并回收 timer。调度循环醒来即停，不再遗弃挂起的
	// timer。
	NewTimer(d time.Duration) Timer
}

// Timer 是 Clock.NewTimer 返回的可停 timer 面（#174）。
type Timer interface {
	// C 返回到期信号通道。
	C() <-chan time.Time
	// Stop 停止 timer：未触发过返回 true（并保证 C 永不再发信号）；已触发
	// 或已停止返回 false。Stop 幂等可重入。
	Stop() bool
}

// SystemClock 是真实时钟（time.Now + time.Timer）。
type SystemClock struct{}

// Now 返回当前时间。
func (SystemClock) Now() time.Time { return time.Now() }

// After 返回一个在 d 之后收到信号的通道。
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// systemTimer 是 SystemClock 的可停 timer（标准库 time.Timer 的适配面）。
type systemTimer struct {
	t *time.Timer
}

func (t *systemTimer) C() <-chan time.Time { return t.t.C }

func (t *systemTimer) Stop() bool { return t.t.Stop() }

// NewTimer 返回可停 timer（#174）。d <= 0 时标准库立即触发，与 After 的
// 「已就绪通道」契约一致。
func (SystemClock) NewTimer(d time.Duration) Timer { return &systemTimer{t: time.NewTimer(d)} }

// FakeClock 是可手动推进的假时钟，用于确定性测试（不依赖 sleep）。
//
// 典型用法：
//
//	clock := local.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
//	sched, _ := local.New(local.Config{Clock: clock, Location: time.UTC})
//	sched.Schedule("*/5 * * * * *", fn)
//	go sched.Start(ctx)
//	clock.Advance(5 * time.Second) // 触发点到达，fn 被调用
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*fakeWaiter
}

// fakeWaiter 是一个待唤醒的等待者。
type fakeWaiter struct {
	deadline time.Time
	ch       chan time.Time
	// fired 由 c.mu 保护：该等待已触发过（Stop 对它返回 false）。
	fired bool
}

// NewFakeClock 创建以 t 为当前时间的假时钟。
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now 返回假时钟的当前时间。
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 返回等待通道：假时钟推进到该等待的截止时间时，通道收到信号。
//
// d <= 0 时立即返回已就绪的通道（不注册等待者）。
// 经 NewTimer 实现（#174 单源）；After 形态的 timer 不可停是该形态的
// 固有限制，循环等待场景请直接用 NewTimer。
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	return c.NewTimer(d).C()
}

// NewTimer 返回可停 timer（#174）：Stop 把等待者从唤醒队列摘除并保证
// C 永不再发信号。
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	w := &fakeWaiter{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	t := &fakeTimer{clock: c, w: w}
	if d <= 0 {
		w.ch <- w.deadline
		w.fired = true
		return t
	}
	c.waiters = append(c.waiters, w)
	return t
}

// fakeTimer 是 FakeClock 的可停 timer 面（#174）。
type fakeTimer struct {
	clock *FakeClock
	w     *fakeWaiter
}

func (t *fakeTimer) C() <-chan time.Time { return t.w.ch }

// Stop 把等待者摘出唤醒队列（幂等）：未触发过返回 true，摘除后 C 永不再
// 收到信号。
func (t *fakeTimer) Stop() bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.w.fired {
		return false
	}
	for i, w := range c.waiters {
		if w == t.w {
			t.w.fired = true // 复用 fired 标记「已离开等待队列」
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return true
		}
	}
	return false
}

// Advance 推进假时钟 d，并唤醒所有已到期的等待者。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	fired := make([]*fakeWaiter, 0, len(c.waiters))
	rest := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.deadline.After(now) {
			w.fired = true
			fired = append(fired, w)
			continue
		}
		rest = append(rest, w)
	}
	c.waiters = rest
	c.mu.Unlock()

	for _, w := range fired {
		select {
		case w.ch <- w.deadline:
		default:
		}
	}
}
