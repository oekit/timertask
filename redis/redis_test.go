package redis

import (
	"encoding/json"
	"testing"

	"github.com/oekit/timertask"
)

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

func TestParseTask(t *testing.T) {
	b := &RedisBackend{}
	task := &timertask.Task{
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
