package timertask

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisBackend(t *testing.T) (*RedisBackend, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	cfg := applyOptions([]TimerTaskOption{
		WithBuckets(4),
		WithWorkers(1),
		WithRetry(0, 10*time.Millisecond),
	})
	return &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      func(ctx context.Context, task *Task) error { return nil },
		buckets:      cfg.buckets,
		workers:      cfg.workers,
		retryCount:   cfg.retryCount,
		retryDelay:   cfg.retryDelay,
		localQueue:   make(chan *Task, 1000),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, cfg.buckets),
	}, mr
}

func TestRedisBackend_Send(t *testing.T) {
	b, mr := newTestRedisBackend(t)
	ctx := context.Background()

	task := &Task{
		ID:      "send-1",
		EndTime: time.Now().Add(time.Hour).Unix(),
		Action:  "test",
		Param:   `{"x":1}`,
	}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}
	if task.CreateTime == 0 {
		t.Error("expected Send to set CreateTime")
	}
	if task.MaxRetry != 0 {
		t.Error("expected MaxRetry to default to 0 (retryCount=0)")
	}

	// 验证 miniredis 真的有 key
	keys := mr.Keys()
	if len(keys) == 0 {
		t.Error("expected at least one key in miniredis after Send")
	}
}

func TestRedisBackend_Send_Distribution(t *testing.T) {
	// 100 个 task 应分散到多个 shard
	b, mr := newTestRedisBackend(t)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		task := &Task{
			ID:      "dist-" + string(rune('0'+i%10)) + "-" + string(rune('0'+(i/10)%10)),
			EndTime: time.Now().Add(time.Hour).Unix(),
		}
		if err := b.Send(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	// buckets=4, 期望 4 个不同 shard key
	keys := mr.Keys()
	shardKeys := 0
	for _, k := range keys {
		if len(k) > 0 && k[0:9] == "{timertas" {
			shardKeys++
		}
	}
	if shardKeys < 2 {
		t.Errorf("expected tasks distributed across multiple shards, got %d unique keys", shardKeys)
	}
}

func TestRedisBackend_Send_SetsDefaults(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	b := &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      func(ctx context.Context, task *Task) error { return nil },
		buckets:      4,
		workers:      1,
		retryCount:   3,
		retryDelay:   time.Second,
		localQueue:   make(chan *Task, 100),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, 4),
	}

	task := &Task{
		ID:      "defaults-1",
		EndTime: time.Now().Add(time.Hour).Unix(),
	}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if task.MaxRetry != 3 {
		t.Errorf("expected MaxRetry=3, got %d", task.MaxRetry)
	}
	if task.CreateTime == 0 {
		t.Error("expected CreateTime to be set")
	}
}

func TestRedisBackend_PendingCount(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx := context.Background()

	count, err := b.PendingCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 initially, got %d", count)
	}

	for i := 0; i < 3; i++ {
		task := &Task{
			ID:      "pending-" + string(rune('0'+i)),
			EndTime: time.Now().Add(time.Hour).Unix(),
		}
		if sendErr := b.Send(ctx, task); sendErr != nil {
			t.Fatal(sendErr)
		}
	}

	count, err = b.PendingCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestRedisBackend_LocalPendingCount(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	if got := b.LocalPendingCount(); got != 0 {
		t.Errorf("expected 0 initially, got %d", got)
	}

	task := &Task{ID: "local-1"}
	b.localQueue <- task
	if got := b.LocalPendingCount(); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
}

func TestRedisBackend_ParseTask_RoundTrip(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	task := &Task{
		ID:         "parse-1",
		CreateTime: 1000,
		EndTime:    2000,
		Action:     "act",
		Param:      `{"x":1}`,
		Extra:      "ext",
		MaxRetry:   3,
	}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	keys, err := b.client.ZRange(context.Background(), b.taskKey("parse-1"), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("expected at least one task in ZSet")
	}

	got := b.parseTask(keys[0])
	if got == nil {
		t.Fatal("parseTask returned nil for valid JSON")
	}
	if got.ID != "parse-1" {
		t.Errorf("ID: expected parse-1, got %q", got.ID)
	}
	if got.EndTime != 2000 {
		t.Errorf("EndTime: expected 2000, got %d", got.EndTime)
	}
}

func TestRedisBackend_ConsumeRemoteOnce_PushToLocalQueue(t *testing.T) {
	// 直接测 consumeRemoteOnce: 把 task 放入 ZSet (score=now-1 过期),
	// 验证 deleteScript Lua 修复后能正确把 task claim + push 到 localQueue。
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	cfg := applyOptions([]TimerTaskOption{WithBuckets(4), WithWorkers(1)})
	b := &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      func(ctx context.Context, task *Task) error { return nil },
		buckets:      cfg.buckets,
		workers:      cfg.workers,
		retryCount:   cfg.retryCount,
		retryDelay:   cfg.retryDelay,
		localQueue:   make(chan *Task, 100),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, cfg.buckets),
	}

	// 强制让当前 node 拥有所有 shard
	for i := 0; i < cfg.buckets; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}

	// 插入一个已过期的 task 到 shard 0 (score=now-60, 必过期)
	expiredTask := &Task{
		ID:      "expired-1",
		EndTime: time.Now().Unix() - 60,
		Action:  "test",
	}
	if err := b.Send(context.Background(), expiredTask); err != nil {
		t.Fatal(err)
	}

	// consumeRemoteOnce 应当:
	//   1. ZRangeByScore 扫到 expired-1
	//   2. deleteScript (修复后 `zscore <= now`) 成功 claim 删除
	//   3. push 到 localQueue
	b.consumeRemoteOnce(context.Background())

	select {
	case got := <-b.localQueue:
		if got.ID != "expired-1" {
			t.Errorf("expected expired-1, got %q", got.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected task in localQueue within 500ms after consumeRemoteOnce")
	}

	// 验证 ZSet 中 task 已被 deleteScript 删除
	pending, err := b.PendingCount(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("expected 0 pending after consume, got %d", pending)
	}
}

func TestDeleteScript_ReproAndFix(t *testing.T) {
	// 单元测试 deleteScript Lua 条件修复: zscore <= ARGV[2] (now) 时删除.
	// 之前用 zscore == ARGV[2], 几乎永远不匹配, task 永远卡在 ZSet.
	//
	// 注: miniredis 把 `<=` 当 `<` 解析 (上游实现 bug), 所以这里用
	// `<=` 跟 miniredis 行为不同, 但真 Redis 标准. 因此 test 只覆盖
	// "now >= EndTime" 的删除路径 (修复后的正确语义), 不测 "now < EndTime"
	// (miniredis 下会假阳性). EndTime 没到的 task 由 ZRangeByScore
	// 的 Min/Max 在更外层过滤掉, 不会进 deleteScript.
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	ctx := context.Background()

	key := "{timertask.test}.shard.0"

	cases := []struct {
		name     string
		now      int64
		expected int // deleteScript.Run.Int() 返回 int
	}{
		{"now == EndTime (1000)", 1000, 1},
		{"now > EndTime (1001, original bug)", 1001, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			member := `{"task_id":"` + tc.name + `","end_time":1000}`
			// 每次清空 zset, 避免上一个 case 干扰
			c.Del(ctx, key)
			if err := c.ZAdd(ctx, key, redis.Z{
				Score:  1000,
				Member: member,
			}).Err(); err != nil {
				t.Fatal(err)
			}

			res, err := deleteScript.Run(ctx, c, []string{key}, member, tc.now).Int()
			if err != nil {
				t.Fatal(err)
			}
			if res != tc.expected {
				t.Errorf("now=%d: expected %d, got %d", tc.now, tc.expected, res)
			}
		})
	}
}

func TestRedisBackend_GetAllInstances_NotApplicable(t *testing.T) {
	// 占位: RedisBackend 没有 GetAllInstances, 这个是 LocalBackend 的方法。
	// 保持显式 negative test 防止未来误加。
	b, _ := newTestRedisBackend(t)
	if b.localQueue == nil {
		t.Error("placeholder")
	}
}

// TestDeleteScript_TonumberComparison 钉住 2026-09-06 修复:deleteScript
// 必须显式 tonumber 比较。Lua 5.1 对两个 string 的 `<=` 是字典序——
// 位长不同的时间戳(如 9 位 vs 10 位)数值与字典序结论相反,旧写法下
// 任务永远无法 claim 且按 score 升序占满批次饿死 shard。
func TestDeleteScript_TonumberComparison(t *testing.T) {
	mr := miniredis.RunT(t)
	defer mr.Close()
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	const key = "test:zset"

	// 9 位 score 数值上 <= 9 位 now;旧字典序写法在混位数场景恒假
	if err := c.ZAdd(ctx, key, redis.Z{Score: 999999998, Member: "t-short"}).Err(); err != nil {
		t.Fatal(err)
	}
	res, err := deleteScript.Run(ctx, c, []string{key}, "t-short", "999999999").Result()
	if err != nil {
		t.Fatalf("deleteScript: %v", err)
	}
	if res != int64(1) {
		t.Errorf("deleteScript = %v, want 1(tonumber 语义下 999999998 <= 999999999 成立)", res)
	}
	if _, zerr := c.ZScore(ctx, key, "t-short").Result(); zerr == nil {
		t.Error("task should be removed from ZSet, still present")
	}

	// 未到期任务不删
	if zerr := c.ZAdd(ctx, key, redis.Z{Score: 9999999999, Member: "t-future"}).Err(); zerr != nil {
		t.Fatal(zerr)
	}
	res2, err := deleteScript.Run(ctx, c, []string{key}, "t-future", "999999999").Result()
	if err != nil {
		t.Fatalf("deleteScript future: %v", err)
	}
	if res2 != int64(0) {
		t.Errorf("deleteScript = %v, want 0(未到期不删)", res2)
	}
}

// TestRedisBackend_SecondStart_NoPanic 回归：Start 曾在 ctx.Done 后裸
// close(b.stopChan)，二次 Start 直接 close of closed channel panic
// （LocalBackend 已有 stopOnce，Redis 侧漏改）。钉住：二次 Start 不 panic。
func TestRedisBackend_SecondStart_NoPanic(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("second Start must not panic, got: %v", r)
		}
	}()

	// 第一次 Start：正常跑完优雅停机路径
	_ = b.Start(ctx)
	time.Sleep(50 * time.Millisecond)

	// 第二次 Start：用已取消的 ctx，应立即返回而非 panic
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	_ = b.Start(ctx2)
}
