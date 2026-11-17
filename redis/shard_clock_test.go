package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// TestShardManager_Heartbeat_FakeClockTimestampAndExpiry 钉住 #260：分片面的
// 时间判断（心跳注册时间戳 + 过期清理阈值）单源走注入时钟——#152 声明
// 「任务时间判断单源走 b.now()」后分片面完全绕开，心跳 TTL 判定/过期清理
// 的测试只能真睡。注入 FakeClock 后全部确定性断言。
//
// 复现（确定性）：假时钟钉在真实时钟的遥远过去（2023），心跳条目 score =
// 假时刻。若心跳时间戳或过期阈值任一侧走真实时钟，阈值 = 真实 now - 5s
// 远大于假 score——自己的条目与新鲜 peer 会被立刻清掉（红）；两侧都走
// 注入时钟则自己的条目保留、只有过期 peer 被清（绿）。
func TestShardManager_Heartbeat_FakeClockTimestampAndExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	// 假时钟钉在 2023（相对真实时钟在遥远过去），阈值口径由它决定。
	now := time.Unix(1700000000, 0)
	s := NewShardManager("clk-ttl", c, 16, WithShardClock(&fakeClock{now: now}))
	key := s.heartbeatKey()

	seed := func(member string, age time.Duration) {
		t.Helper()
		if err := c.ZAdd(context.Background(), key, goredis.Z{
			Score:  float64(now.Add(-age).UnixMilli()),
			Member: member,
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	seed("stale-peer", 6*time.Second)    // 超过 heartbeatTimeout(5s)：该清
	seed("fresh-peer", 1*time.Second)    // 未过期：该留
	seed(s.GetClientID(), 6*time.Second) // 自己的旧条目：ZAdd 同 member 覆盖

	s.heartbeat(context.Background())

	// 心跳时间戳走注入时钟：自己的条目 score = 假时刻（毫秒）。
	score, err := c.ZScore(context.Background(), key, s.GetClientID()).Result()
	if err != nil {
		t.Fatalf("自己的心跳条目丢失（若时间戳/阈值走了真实时钟，假 score 会被真实阈值立刻清掉）: %v", err)
	}
	if want := float64(now.UnixMilli()); score != want {
		t.Fatalf("心跳 score = %v, want %v（#260：时间戳必须走注入时钟）", score, want)
	}

	// 过期清理阈值走注入时钟：过期 peer 被清、新鲜 peer 保留。
	members, err := c.ZRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, m := range members {
		got[m] = true
	}
	if got["stale-peer"] {
		t.Fatal("过期 peer 未被清理（阈值未随注入时钟推进）")
	}
	if !got["fresh-peer"] {
		t.Fatal("新鲜 peer 被误清（若阈值走了真实时钟，假 score 会被判过期）")
	}
	if !got[s.GetClientID()] {
		t.Fatal("自己的条目被误清")
	}
}

// TestShardManager_Heartbeat_FakeClockExpiryAdvances 钉住过期清理随注入时钟
// 推进：peer 条目在假时钟未推进时保留，推进超过 heartbeatTimeout 后被下一拍
// 心跳清掉（不真睡、不依赖真实时间流逝）。
func TestShardManager_Heartbeat_FakeClockExpiryAdvances(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	now := time.Unix(1700000000, 0)
	fake := &fakeClock{now: now}
	s := NewShardManager("clk-ttl2", c, 16, WithShardClock(fake))
	key := s.heartbeatKey()

	if err := c.ZAdd(context.Background(), key, goredis.Z{
		Score:  float64(now.UnixMilli()),
		Member: "peer",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	// 假时钟未推进：peer 不算过期。
	s.heartbeat(context.Background())
	n, err := c.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("未推进时钟不应清理 peer：members = %d, want 2", n)
	}

	// 推进 6s（超过 heartbeatTimeout 5s）：下一拍心跳清掉 peer。
	fake.now = now.Add(6 * time.Second)
	s.heartbeat(context.Background())
	n, err = c.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("时钟推进 6s 后 peer 应被清理：members = %d, want 1", n)
	}
}

// TestNewRedisBackend_ClockPropagatesToShardManager 钉住 #260 的下发面：
// timertask.WithClock 经 ResolveOptions 一路下发到 ShardManager——分片面的
// 时间判断与任务时间判断同源；缺省（nil）回退真实时钟。
func TestNewRedisBackend_ClockPropagatesToShardManager(t *testing.T) {
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	now := time.Unix(1700000000, 0)
	fake := &fakeClock{now: now}
	b := NewRedisBackend("clk-prop", c, func(context.Context, *timertask.Task) error { return nil },
		timertask.WithBuckets(4),
		timertask.WithClock(fake),
	)
	if got := b.shardManager.now(); !got.Equal(now) {
		t.Fatalf("shardManager.now() = %v, want 注入时钟 %v（#260：WithClock 必须下发到分片面）", got, now)
	}

	// 缺省 nil 回退真实时钟（与 b.now 同口径）。
	def := NewShardManager("clk-def", c, 16)
	realBefore := time.Now()
	got := def.now()
	realAfter := time.Now()
	if got.Before(realBefore) || got.After(realAfter) {
		t.Fatalf("缺省时钟应回退真实时钟，got %v 不在 [%v, %v]", got, realBefore, realAfter)
	}
}
