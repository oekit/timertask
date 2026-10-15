package timertask

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRedisBackend_HandlerPanic_TreatedAsFailure 回归：processTask 裸调 handler
// 时，一次 panic 会穿透 workerLoop 打死该 worker goroutine（async.GoSafe 的
// recover 在 goroutine 出口、不重启），N 个 panic 任务耗尽 worker 池后消费瘫痪，
// 且任务已从 ZSet claim、不会进死信 ZSet——永久丢失。钉住：panic 就地收编为
// 任务失败，走重试且 worker 存活。
func TestRedisBackend_HandlerPanic_TreatedAsFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	var attempts atomic.Int32
	handler := func(ctx context.Context, task *Task) error {
		if attempts.Add(1) == 1 {
			panic("handler boom")
		}
		return nil // 重试成功
	}
	cfg := applyOptions([]TimerTaskOption{
		WithBuckets(4),
		WithWorkers(1),
		WithRetry(1, 10*time.Millisecond),
	})
	b := &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      handler,
		buckets:      cfg.buckets,
		workers:      cfg.workers,
		retryCount:   cfg.retryCount,
		retryDelay:   cfg.retryDelay,
		localQueue:   make(chan *Task, 1000),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, cfg.buckets),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	if err := b.Send(ctx, &Task{
		ID:       "panic-1",
		EndTime:  time.Now().Unix(),
		Action:   "test",
		MaxRetry: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// 第一次 panic 后应重试并成功
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected retry after handler panic, got %d attempts", got)
	}
}

// TestRedisBackend_HandlerPanicAlways_DeadLetter 钉住：恒 panic 的任务重试耗尽后
// 必须写入持久死信 ZSet（修复前 panic 直接打死 worker，任务已 claim 却静默丢失）。
func TestRedisBackend_HandlerPanicAlways_DeadLetter(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	handler := func(ctx context.Context, task *Task) error {
		panic("handler always boom")
	}
	cfg := applyOptions([]TimerTaskOption{
		WithBuckets(4),
		WithWorkers(1),
		WithRetry(0, 10*time.Millisecond),
	})
	b := &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      handler,
		buckets:      cfg.buckets,
		workers:      cfg.workers,
		retryCount:   cfg.retryCount,
		retryDelay:   cfg.retryDelay,
		localQueue:   make(chan *Task, 1000),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, cfg.buckets),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	if err := b.Send(ctx, &Task{
		ID:       "panic-dead-1",
		EndTime:  time.Now().Unix(),
		Action:   "test",
		MaxRetry: 0,
	}); err != nil {
		t.Fatal(err)
	}

	// consumeRemoteLoop 500ms 一轮，留足等待窗口
	deadKey := "{timertask.test}.dead"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := c.ZCard(context.Background(), deadKey).Result()
		if err == nil && n == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected task in dead letter ZSet %s after panic exhaustion", deadKey)
}

// TestRedisBackend_NegativeMaxRetry_Normalized 回归：负 MaxRetry 的任务此前
// "未执行即进死信"（循环体不执行、err 保持 nil），handler 从未运行却产生持久
// 死信。钉住：负值归一为 0，至少执行一次。
func TestRedisBackend_NegativeMaxRetry_Normalized(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	var executed atomic.Int32
	handler := func(ctx context.Context, task *Task) error {
		executed.Add(1)
		return nil
	}
	cfg := applyOptions([]TimerTaskOption{WithBuckets(4), WithWorkers(1)})
	b := &RedisBackend{
		client:       c,
		buzType:      "test",
		handler:      handler,
		buckets:      cfg.buckets,
		workers:      cfg.workers,
		retryCount:   cfg.retryCount,
		retryDelay:   cfg.retryDelay,
		localQueue:   make(chan *Task, 1000),
		stopChan:     make(chan struct{}),
		shardManager: NewShardManager("test", c, cfg.buckets),
	}

	b.processTask(context.Background(), &Task{ID: "neg-1", MaxRetry: -1})
	if executed.Load() != 1 {
		t.Fatalf("expected handler executed exactly once for negative MaxRetry, got %d", executed.Load())
	}
}
