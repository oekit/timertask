package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// TestConsumeRemoteOnce_StopDuringClaimRequeues 钉住 #96：claim（at-most-once
// 的 deleteScript 原子删除 / at-least-once 的 claimLeaseScript 移入租约集）与
// 入队之间遇停机信号，在途任务必须回灌 Redis——直接 return 会把它丢在
// 「既不在 ZSet 也不在 localQueue」的地方，requeuePendingOnShutdown 只回灌
// localQueue 里的任务，捡不到这一份，兜底失效。
//
// 复现（确定性，无 sleep、无竞态窗口）：localQueue 塞满（容量 1）让入队发送
// 必然阻塞，stopChan 预先关闭让 select 唯一就绪分支是停机退出——claim 照常
// 完成（claim 路径不看 stopChan），入队永远走不到，正好落在停机窗口上。
func TestConsumeRemoteOnce_StopDuringClaimRequeues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		atLeast bool
	}{
		{"at-most-once", false},
		{"at-least-once", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = c.Close() })

			opts := []timertask.TimerTaskOption{
				timertask.WithBuckets(4),
				timertask.WithWorkers(1),
			}
			if tc.atLeast {
				opts = append(opts, timertask.WithAtLeastOnce(30*time.Second))
			}
			cfg := timertask.ResolveOptions(opts...)
			b := &RedisBackend{
				client:            c,
				buzType:           "stopwin",
				handler:           func(ctx context.Context, task *timertask.Task) error { return nil },
				buckets:           cfg.Buckets,
				workers:           cfg.Workers,
				retryCount:        cfg.RetryCount,
				retryDelay:        cfg.RetryDelay,
				delivery:          cfg.Delivery,
				visibilityTimeout: cfg.VisibilityTimeout,
				localQueue:        make(chan *claim, 1),
				stopChan:          make(chan struct{}),
				shardManager:      NewShardManager("stopwin", c, cfg.Buckets),
			}
			// 不起 Start（心跳/消费循环都不在），手动持有全部分片
			for i := 0; i < cfg.Buckets; i++ {
				b.shardManager.myShards = append(b.shardManager.myShards, i)
			}

			// 塞满 localQueue：claim 后的入队发送必然阻塞
			b.localQueue <- &claim{task: &timertask.Task{ID: "blocker"}, member: "blocker"}
			// 停机信号已到：发送 select 只剩 stopChan 分支就绪
			close(b.stopChan)

			task := &timertask.Task{ID: "inflight-1", EndTime: time.Now().Unix() - 60, MaxRetry: 0}
			if err := b.Send(context.Background(), task); err != nil {
				t.Fatal(err)
			}

			b.consumeRemoteOnce(context.Background())

			// 在途任务必须回灌 ZSet（旧实现：既不在 ZSet 也不在 localQueue，静默丢弃）
			pending, err := b.PendingCount(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if pending != 1 {
				t.Fatalf("pending = %d, want 1（claim 后停机须回灌，#96）", pending)
			}
			// localQueue 仍只有 blocker：在途任务没有被塞进已停机的队列等死
			if got := b.LocalPendingCount(); got != 1 {
				t.Fatalf("localQueue = %d, want 1", got)
			}
			if tc.atLeast {
				n, _ := c.ZCard(context.Background(), b.processingKey()).Result()
				if n != 0 {
					t.Fatalf("processing = %d, want 0（回灌须同时摘掉租约）", n)
				}
			}
			// 回灌的条目 member 完好、可被再次消费
			members, err := c.ZRange(context.Background(), b.taskKey("inflight-1"), 0, -1).Result()
			if err != nil || len(members) != 1 {
				t.Fatalf("read member: %v (%d)", err, len(members))
			}
			if got := b.parseTask(members[0]); got == nil || got.ID != "inflight-1" {
				t.Fatalf("回灌 member 损坏: %+v", got)
			}
		})
	}
}
