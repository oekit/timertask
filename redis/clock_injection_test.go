package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

// TestNewRedisBackend_WithClock_ControlsRedisTimeSource 钉住 #152：WithClock
// 对 RedisBackend 不得是静默 no-op——consumeRemoteOnce 的「到点」判断必须走
// 注入时钟，FakeClock 注入后不真睡即可断言「到点触发」。
//
// 缺陷成因：BackendConfig 没有 Clock 字段、ResolveOptions 不下发 cfg.clock，
// redis 侧时间判断硬编码 time.Now()——timertask.WithClock(fake) 传入即被
// 静默吞掉（#78 只修了 LocalBackend 侧）。
//
// 复现（确定性）：假时钟钉在 T=1700000000（相对真实时钟在遥远过去），任务
// EndTime = T+1h——相对假时钟未到点、相对真实时钟早已过期。若时间判断走
// 真实时钟，任务会被立刻消费（红）；走注入时钟则不触发（绿）。
func TestNewRedisBackend_WithClock_ControlsRedisTimeSource(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	now := time.Unix(1700000000, 0)
	fake := &fakeClock{now: now}
	b := NewRedisBackend("clk", c, func(ctx context.Context, task *timertask.Task) error { return nil },
		timertask.WithBuckets(4),
		timertask.WithClock(fake),
	)
	for i := 0; i < 4; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}

	// 相对假时钟未到点（相对真实时钟早已过期）的任务
	if err := b.Send(context.Background(), &timertask.Task{ID: "clk-1", EndTime: now.Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	b.consumeRemoteOnce(context.Background())
	if got := b.LocalPendingCount(); got != 0 {
		t.Fatalf("未到点任务被消费：%d（#152：Redis 侧时间判断必须走注入时钟，而非 time.Now）", got)
	}

	// 推进假时钟到触发点之后（不真睡）→ 到点消费
	fake.now = now.Add(2 * time.Hour)
	b.consumeRemoteOnce(context.Background())
	if got := b.LocalPendingCount(); got != 1 {
		t.Fatalf("时钟推进后任务未被消费：%d（#152：到点判断须随注入时钟推进）", got)
	}
}
