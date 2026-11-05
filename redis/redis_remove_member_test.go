package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/oekit/timertask"
)

// TestRedisBackend_Send_MemberCanonical member 规范化（#75-②）：同任务同 fire_ts
// 拍两次 Send（CreateTime 不同）⇒ ZSet 单条；消费侧 CreateTime==EndTime。
func TestRedisBackend_Send_MemberCanonical(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := NewRedisBackend("canon", client, func(ctx context.Context, task *timertask.Task) error { return nil })
	ctx := context.Background()

	fire := time.Now().Add(time.Hour).Unix()
	if err := b.Send(ctx, &timertask.Task{ID: "dup-1", EndTime: fire}); err != nil {
		t.Fatal(err)
	}
	// 第二拍：显式给不同的 CreateTime（旧实现下 member 不同 ⇒ 两条 ⇒ 双跑）
	if err := b.Send(ctx, &timertask.Task{ID: "dup-1", EndTime: fire, CreateTime: 123456789}); err != nil {
		t.Fatal(err)
	}

	key := b.taskKey("dup-1")
	n, err := client.ZCard(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("同拍重发后 ZCard=%d, want 1（同 member 更新而非新增）", n)
	}

	members, err := client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	var got timertask.Task
	if err := json.Unmarshal([]byte(members[0]), &got); err != nil {
		t.Fatal(err)
	}
	if got.CreateTime != got.EndTime {
		t.Fatalf("member 里 CreateTime=%d, want ==EndTime=%d（规范化钉死）", got.CreateTime, got.EndTime)
	}
}

// TestRedisBackend_SameShotResend_NoDoubleFire 端到端：同拍重发只触发一次。
func TestRedisBackend_SameShotResend_NoDoubleFire(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	var fired int
	b := NewRedisBackend("dup", client, func(ctx context.Context, task *timertask.Task) error {
		fired++
		return nil
	}, timertask.WithBuckets(4))
	for i := 0; i < 4; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}

	fire := time.Now().Unix() - 1
	if err := b.Send(context.Background(), &timertask.Task{ID: "dup-fire", EndTime: fire, MaxRetry: 0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(context.Background(), &timertask.Task{ID: "dup-fire", EndTime: fire, CreateTime: 42, MaxRetry: 0}); err != nil {
		t.Fatal(err)
	}

	b.consumeRemoteOnce(context.Background())
	if b.LocalPendingCount() != 1 {
		t.Fatalf("同拍重发应只入本地队列 1 条, got %d", b.LocalPendingCount())
	}
}

// TestRedisBackend_Remove_CancelsPending 撤约（#75-①）：未触发的任务 Remove 后
// 不再进入消费队列；重复 Remove 幂等返回 0。
func TestRedisBackend_Remove_CancelsPending(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := NewRedisBackend("cancel", client, func(ctx context.Context, task *timertask.Task) error { return nil })
	ctx := context.Background()

	fire := time.Now().Add(time.Hour).Unix()
	if err := b.Send(ctx, &timertask.Task{ID: "cancel-1", EndTime: fire}); err != nil {
		t.Fatal(err)
	}
	if pending, _ := b.PendingCount(ctx); pending != 1 {
		t.Fatalf("pending=%d, want 1", pending)
	}

	removed, err := b.Remove(ctx, "cancel-1", fire)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want 1", removed)
	}
	if pending, _ := b.PendingCount(ctx); pending != 0 {
		t.Fatalf("pending=%d, want 0 after remove", pending)
	}
	if n, _ := client.ZCard(ctx, b.taskKey("cancel-1")).Result(); n != 0 {
		t.Fatalf("zcard=%d, want 0", n)
	}

	// 幂等：再撤一次返回 0 且无错
	removed, err = b.Remove(ctx, "cancel-1", fire)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("idempotent remove=%d, want 0", removed)
	}
}

// TestRedisBackend_Remove_UnknownOrFired 未知任务 / 空 ID 的边界。
func TestRedisBackend_Remove_UnknownOrFired(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx := context.Background()

	removed, err := b.Remove(ctx, "no-such-task", time.Now().Unix())
	if err != nil {
		t.Fatalf("unknown task should be a no-op, got err=%v", err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d, want 0", removed)
	}

	if _, err := b.Remove(ctx, "", time.Now().Unix()); err == nil {
		t.Fatal("empty taskID should be rejected")
	}
}

// TestRedisBackend_Remove_OnlyExactFireTS 同 ID 不同 fire_ts：只撤指定拍。
func TestRedisBackend_Remove_OnlyExactFireTS(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx := context.Background()

	fireA := time.Now().Add(time.Hour).Unix()
	fireB := time.Now().Add(2 * time.Hour).Unix()
	if err := b.Send(ctx, &timertask.Task{ID: "multi", EndTime: fireA}); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(ctx, &timertask.Task{ID: "multi", EndTime: fireB}); err != nil {
		t.Fatal(err)
	}

	removed, err := b.Remove(ctx, "multi", fireA)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want 1", removed)
	}
	if pending, _ := b.PendingCount(ctx); pending != 1 {
		t.Fatalf("pending=%d, want 1（fireB 的那一拍必须保留）", pending)
	}
}

// TestRedisBackend_Remove_LegacyMember 旧版全量 JSON member（CreateTime≠EndTime）
// 一样按 ID 匹配可撤销——升级过渡期不破坏撤约。
func TestRedisBackend_Remove_LegacyMember(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := NewRedisBackend("legacy", client, func(ctx context.Context, task *timertask.Task) error { return nil })
	ctx := context.Background()

	fire := time.Now().Add(time.Hour).Unix()
	legacy, err := json.Marshal(&timertask.Task{ID: "legacy-1", EndTime: fire, CreateTime: fire - 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ZAdd(ctx, b.taskKey("legacy-1"), goredis.Z{Score: float64(fire), Member: string(legacy)}).Result(); err != nil {
		t.Fatal(err)
	}

	removed, err := b.Remove(ctx, "legacy-1", fire)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want 1（旧格式 member 也可撤销）", removed)
	}
}
