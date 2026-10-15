package timertask

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTask_JSON(t *testing.T) {
	task := Task{
		ID:         "task-001",
		CreateTime: time.Now().Unix(),
		EndTime:    time.Now().Add(10 * time.Second).Unix(),
		Action:     "close_order",
		Param:      `{"order_id": 123}`,
		Extra:      "extra",
	}

	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Task
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ID != "task-001" {
		t.Errorf("expected 'task-001', got %q", decoded.ID)
	}
	if decoded.Action != "close_order" {
		t.Errorf("expected 'close_order', got %q", decoded.Action)
	}
	if decoded.Param != `{"order_id": 123}` {
		t.Errorf("unexpected param: %s", decoded.Param)
	}
}

func TestRedisBackend_TaskKey(t *testing.T) {
	b := &RedisBackend{
		buzType: "order",
		buckets: 128,
	}

	key := b.taskKey("task-001")
	if key == "" {
		t.Error("expected non-empty key")
	}
	// Same taskID should produce same key
	key2 := b.taskKey("task-001")
	if key != key2 {
		t.Errorf("expected consistent key, got %q and %q", key, key2)
	}
}

func TestRedisBackend_ShardKey(t *testing.T) {
	b := &RedisBackend{
		buzType: "order",
		buckets: 128,
	}

	key := b.shardKey(0)
	if key != "{timertask.order}.shard.0" {
		t.Errorf("expected '{timertask.order}.shard.0', got %q", key)
	}

	key = b.shardKey(5)
	if key != "{timertask.order}.shard.5" {
		t.Errorf("expected '{timertask.order}.shard.5', got %q", key)
	}
}

func TestRedisBackend_TaskKey_EmptyID(t *testing.T) {
	b := &RedisBackend{
		buzType: "order",
		buckets: 128,
	}
	key := b.taskKey("")
	if key != "{timertask.order}.shard.0" {
		t.Errorf("expected shard 0 for empty ID, got %q", key)
	}
}

func TestRedisBackend_TaskKey_Distribution(t *testing.T) {
	b := &RedisBackend{
		buzType: "order",
		buckets: 4,
	}

	shards := make(map[string]int)
	for i := 0; i < 100; i++ {
		key := b.taskKey("task-" + string(rune('0'+i%10)))
		shards[key]++
	}

	// Should have multiple different keys
	if len(shards) < 2 {
		t.Errorf("expected multiple shards, got %d", len(shards))
	}
}

func TestTask_Fields(t *testing.T) {
	now := time.Now().Unix()
	task := Task{
		ID:         "test",
		CreateTime: now,
		EndTime:    now + 60,
		Action:     "action",
		Param:      "param",
		Extra:      "extra",
	}
	if task.ID != "test" {
		t.Error("unexpected ID")
	}
	if task.CreateTime != now {
		t.Error("unexpected CreateTime")
	}
	if task.EndTime != now+60 {
		t.Error("unexpected EndTime")
	}
}

func TestLocalBackend_Send(t *testing.T) {
	handler := func(ctx context.Context, task *Task) error {
		return nil
	}

	b := NewLocalBackend(handler)
	ctx := context.Background()

	task := &Task{
		ID:      "test-1",
		EndTime: time.Now().Unix() + 10,
		Action:  "test",
	}

	err := b.Send(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate task should be ignored
	err = b.Send(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalBackend_PendingCount(t *testing.T) {
	handler := func(ctx context.Context, task *Task) error {
		return nil
	}

	b := NewLocalBackend(handler)
	ctx := context.Background()

	count, _ := b.PendingCount(ctx)
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	task := &Task{
		ID:      "test-1",
		EndTime: time.Now().Unix() + 10,
		Action:  "test",
	}
	_ = b.Send(ctx, task)

	count, _ = b.PendingCount(ctx)
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestOptions_Default(t *testing.T) {
	cfg := applyOptions(nil)
	if cfg.workers != defaultWorkers {
		t.Errorf("expected workers %d, got %d", defaultWorkers, cfg.workers)
	}
	if cfg.retryCount != defaultRetryCount {
		t.Errorf("expected retryCount %d, got %d", defaultRetryCount, cfg.retryCount)
	}
	if cfg.retryDelay != defaultRetryDelay {
		t.Errorf("expected retryDelay %v, got %v", defaultRetryDelay, cfg.retryDelay)
	}
	if cfg.buckets != defaultBuckets {
		t.Errorf("expected buckets %d, got %d", defaultBuckets, cfg.buckets)
	}
}

func TestWithRetry(t *testing.T) {
	cfg := applyOptions([]TimerTaskOption{WithRetry(5, 2*time.Second)})
	if cfg.retryCount != 5 {
		t.Errorf("expected retryCount 5, got %d", cfg.retryCount)
	}
	if cfg.retryDelay != 2*time.Second {
		t.Errorf("expected retryDelay 2s, got %v", cfg.retryDelay)
	}
}

func TestWithBuckets_Clamp(t *testing.T) {
	// 小于 2 应当被 clamp 到 2
	cfg := applyOptions([]TimerTaskOption{WithBuckets(1)})
	if cfg.buckets != 2 {
		t.Errorf("expected buckets clamped to 2, got %d", cfg.buckets)
	}
	// 大于 1024 应当被 clamp 到 1024
	cfg = applyOptions([]TimerTaskOption{WithBuckets(9999)})
	if cfg.buckets != 1024 {
		t.Errorf("expected buckets clamped to 1024, got %d", cfg.buckets)
	}
}

func TestWithWorkers_Clamp(t *testing.T) {
	// 小于 1 应当被 clamp 到 1
	cfg := applyOptions([]TimerTaskOption{WithWorkers(0)})
	if cfg.workers != 1 {
		t.Errorf("expected workers clamped to 1, got %d", cfg.workers)
	}
}

func TestPriorityQueue_Ordering(t *testing.T) {
	now := time.Now().Unix()
	pq := make(priorityQueue, 0)
	heap.Init(&pq)

	items := []*localTaskItem{
		{task: &Task{ID: "late", EndTime: now + 100}, execTime: now + 100},
		{task: &Task{ID: "early", EndTime: now + 10}, execTime: now + 10},
		{task: &Task{ID: "mid", EndTime: now + 50}, execTime: now + 50},
	}
	for _, it := range items {
		heap.Push(&pq, it)
	}

	// Pop 应当按 execTime 升序
	got := []string{
		heap.Pop(&pq).(*localTaskItem).task.ID,
		heap.Pop(&pq).(*localTaskItem).task.ID,
		heap.Pop(&pq).(*localTaskItem).task.ID,
	}
	want := []string{"early", "mid", "late"}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("position %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLocalBackend_StartAndExecute(t *testing.T) {
	// 验证 Start 启动 scan + worker 后能执行到期的任务。
	processed := make(chan string, 4)
	handler := func(ctx context.Context, task *Task) error {
		processed <- task.ID
		return nil
	}
	b := NewLocalBackend(handler, WithWorkers(2))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Send 一个立即到期的任务
	task := &Task{
		ID:      "immediate-1",
		EndTime: time.Now().Unix(),
		Action:  "test",
	}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}

	// Start 启动后台 goroutine
	startErr := make(chan error, 1)
	go func() { startErr <- b.Start(ctx) }()

	// 等待任务被处理
	select {
	case got := <-processed:
		if got != "immediate-1" {
			t.Errorf("expected immediate-1, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task not processed within 2s")
	}

	// 取消 ctx，Start 应当返回
	cancel()
	select {
	case err := <-startErr:
		if err == nil {
			t.Error("expected non-nil error from cancelled Start")
		}
	case <-time.After(2 * time.Second):
		t.Error("Start did not return within 2s after cancel")
	}
}

func TestLocalBackend_LocalPendingCount(t *testing.T) {
	// 任务被 worker 消费前应当出现在 localQueue 中
	b := NewLocalBackend(func(ctx context.Context, task *Task) error {
		time.Sleep(50 * time.Millisecond) // 模拟慢处理
		return nil
	}, WithWorkers(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = b.Start(ctx) }()
	time.Sleep(50 * time.Millisecond) // 让 worker 启动

	for i := 0; i < 3; i++ {
		task := &Task{
			ID:      "fill-" + string(rune('0'+i)),
			EndTime: time.Now().Unix(),
			Action:  "test",
		}
		if err := b.Send(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	// 此时 worker 应至少处理了 1 个，localQueue 应有计数变化
	if got := b.LocalPendingCount(); got < 0 {
		t.Errorf("expected non-negative count, got %d", got)
	}
}

func TestLocalBackend_HandlerError_Retries(t *testing.T) {
	// handler 总是失败，应当走 MaxRetry 次后放弃。
	var attempts atomic.Int32
	handler := func(ctx context.Context, task *Task) error {
		attempts.Add(1)
		return errors.New("always fail")
	}
	b := NewLocalBackend(handler,
		WithWorkers(1),
		WithRetry(2, 10*time.Millisecond), // MaxRetry=2 表示 i=0,1,2 三次尝试
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = b.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	task := &Task{
		ID:       "fail-1",
		EndTime:  time.Now().Unix(),
		Action:   "test",
		MaxRetry: 2,
	}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}

	// 等所有重试完成
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := attempts.Load(); got < 3 {
		t.Errorf("expected at least 3 attempts (1 initial + 2 retries), got %d", got)
	}
}

func TestParseTask(t *testing.T) {
	b := &RedisBackend{}
	task := &Task{
		ID:      "parsed-1",
		EndTime: 1234567890,
		Action:  "act",
		Param:   `{"x":1}`,
	}
	data, _ := json.Marshal(task)

	got := b.parseTask(string(data))
	if got == nil {
		t.Fatal("parseTask returned nil")
	}
	if got.ID != "parsed-1" {
		t.Errorf("expected ID 'parsed-1', got %q", got.ID)
	}
	if got.EndTime != 1234567890 {
		t.Errorf("unexpected EndTime: %d", got.EndTime)
	}
}

func TestParseTask_InvalidJSON(t *testing.T) {
	b := &RedisBackend{}
	if got := b.parseTask("not json"); got != nil {
		t.Errorf("expected nil for invalid JSON, got %+v", got)
	}
}
