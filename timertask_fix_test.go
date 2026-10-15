package timertask

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestShardManager_HeartbeatFailureGatesAssignment 回归测试（W1-T5）：
// 心跳失败清空分片后，下一次 assignShards 不得基于 ZSET 中的过期条目
// 复活归属（写路径故障、读路径仍可用的场景），直到心跳恢复。
func TestShardManager_HeartbeatFailureGatesAssignment(t *testing.T) {
	b, mr := newTestRedisBackend(t)
	s := b.shardManager
	ctx := context.Background()

	// 心跳成功 → 分配生效
	s.heartbeat(ctx)
	s.assignShards(ctx)
	if len(s.GetMyShards()) == 0 {
		t.Fatal("expected shards to be assigned after successful heartbeat")
	}

	// 模拟写路径故障：禁用写命令（读仍可用，复现原文全场景）
	mr.SetError("READONLY You cannot write against a read only replica")
	s.heartbeat(ctx)

	if shards := s.GetMyShards(); len(shards) != 0 {
		t.Fatalf("expected shards cleared after heartbeat failure, got %v", shards)
	}

	// 关键断言：下一拍分配不得复活分片（修复前会基于自己过期的 ZSET 条目重新分配）
	s.assignShards(ctx)
	if shards := s.GetMyShards(); len(shards) != 0 {
		t.Fatalf("assignment resurrected shards from a stale ZSET view: %v", shards)
	}

	// 心跳恢复后分配恢复
	mr.SetError("")
	s.heartbeat(ctx)
	s.assignShards(ctx)
	if len(s.GetMyShards()) == 0 {
		t.Fatal("expected shards to be re-assigned after heartbeat recovery")
	}
}

// TestShardManager_NoAssignBeforeFirstHeartbeat：首次心跳成功前不得分配
// （此时 ZSET 中没有本客户端条目，分配结果无意义）。
func TestShardManager_NoAssignBeforeFirstHeartbeat(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	s := b.shardManager

	s.assignShards(context.Background())
	if shards := s.GetMyShards(); len(shards) != 0 {
		t.Fatalf("expected no assignment before first successful heartbeat, got %v", shards)
	}
}

// TestRedisBackend_DeadLetterOnFinalFailure 回归测试（W1-T6）：
// 重试耗尽的任务必须写入持久死信 ZSet。
func TestRedisBackend_DeadLetterOnFinalFailure(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx := context.Background()

	b.handler = func(context.Context, *Task) error { return errors.New("always fail") }

	task := &Task{ID: "dead-1", EndTime: time.Now().Add(-time.Hour).Unix(), Action: "x"}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}

	b.processTask(ctx, task)

	dead := "{timertask." + b.buzType + "}.dead"
	members, err := b.client.ZRange(ctx, dead, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange dead letter: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected exactly 1 dead letter, got %d", len(members))
	}
	parsed := b.parseTask(members[0])
	if parsed == nil || parsed.ID != "dead-1" {
		t.Fatalf("dead letter content mismatch: %v", members)
	}
}

// TestRedisBackend_RequeueOnShutdown 回归测试（W1-T6）：
// 优雅停机时本地队列中未处理的任务必须回灌 Redis，而不是随进程丢失。
func TestRedisBackend_RequeueOnShutdown(t *testing.T) {
	b, _ := newTestRedisBackend(t)

	// 直接往本地队列塞一个未处理任务（已被 ZREM 的状态模拟）
	endTime := time.Now().Add(time.Hour).Unix()
	queued := &Task{ID: "requeue-1", EndTime: endTime, Action: "x"}
	b.localQueue <- queued
	if b.LocalPendingCount() != 1 {
		t.Fatal("expected 1 pending local task")
	}

	b.requeuePendingOnShutdown()

	if b.LocalPendingCount() != 0 {
		t.Fatalf("expected local queue drained, got %d", b.LocalPendingCount())
	}

	// 任务必须回到原分片 ZSet（EndTime 保留）
	key := b.taskKey("requeue-1")
	data, err := json.Marshal(queued)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	score, err := b.client.ZScore(context.Background(), key, string(data)).Result()
	if err != nil {
		t.Fatalf("requeued task not found in %s: %v", key, err)
	}
	if int64(score) != endTime {
		t.Fatalf("expected EndTime %d preserved, got %v", endTime, score)
	}
}
