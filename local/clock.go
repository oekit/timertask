package local

import (
	"sync"
	"time"
)

// Clock 是时钟抽象，便于注入假时钟做确定性测试。
//
// 调度器的所有时间判断都走本接口：注册时的基准时间、触发点计算、等待下一个
// 触发点。注入 FakeClock 后，整个调度过程不需要 sleep 即可断言触发序列。
type Clock interface {
	// Now 返回当前时间。
	Now() time.Time
	// After 返回一个通道：在 d 之后收到一个时间信号。
	// d <= 0 时必须返回「已就绪」的通道（调用方据此立即重算，不空等）。
	After(d time.Duration) <-chan time.Time
}

// SystemClock 是真实时钟（time.Now + time.After）。
type SystemClock struct{}

// Now 返回当前时间。
func (SystemClock) Now() time.Time { return time.Now() }

// After 返回一个在 d 之后收到信号的通道。
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

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
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	if d <= 0 {
		ch <- deadline
		return ch
	}
	c.waiters = append(c.waiters, &fakeWaiter{deadline: deadline, ch: ch})
	return ch
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
