package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// newStealBackend 构造一个不起 Start 的后端：手动持有全部分片，claim 路径
// 可由测试逐拍驱动（与 shutdown_requeue_test 同款构造法）。
func newStealBackend(t *testing.T, handler timertask.Handler, opts ...timertask.TimerTaskOption) *RedisBackend {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	b := NewRedisBackend("steal", c, handler, opts...)
	for i := 0; i < b.buckets; i++ {
		b.shardManager.myShards = append(b.shardManager.myShards, i)
	}
	return b
}

// claimDueTask Send 一个已到点任务并消费 claim 出来（已从 ZSet 摘走）。
func claimDueTask(t *testing.T, b *RedisBackend, id string, maxRetry int) *claim {
	t.Helper()
	task := &timertask.Task{ID: id, EndTime: time.Now().Unix() - 1, MaxRetry: maxRetry}
	if err := b.Send(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	b.consumeRemoteOnce(context.Background())
	if b.LocalPendingCount() != 1 {
		t.Fatalf("localQueue = %d, want 1", b.LocalPendingCount())
	}
	return <-b.localQueue
}

// TestProcessTask_InterruptedDuringRetry_AtMostOnce_RequeuesNotDrop 钉住 #151
// 的「静默丢弃」半边：停机窗口内被 worker 抢走的 at-most-once claim，在重试
// 等待中被 ctx 取消打断时，任务必须回灌 Redis——不得裸 return。
//
// 缺陷成因：processTask 重试等待的 case <-ctx.Done(): return 直接退出——
// 任务在 claim 时已从 ZSet 原子删除，也不在 localQueue（回灌捡不到），三处
// 皆无 ⇒ 永久丢失；doc.go「优雅停机时本地队列中尚未处理的任务会回灌 Redis」
// 对被抢走的这批不成立。
//
// 复现（确定性）：claim 后手动驱动 processTask，handler 首次失败即入重试
// 等待（重试延迟 1h 保证唯一就绪分支是 ctx.Done），随后取消 ctx。
func TestProcessTask_InterruptedDuringRetry_AtMostOnce_RequeuesNotDrop(t *testing.T) {
	entered := make(chan struct{}, 1)
	b := newStealBackend(t, func(ctx context.Context, task *timertask.Task) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return errors.New("handler always fails")
	}, timertask.WithRetry(2, time.Hour), timertask.WithBuckets(4))

	c := claimDueTask(t, b, "steal-drop-1", 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.processTask(ctx, c)
	}()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called")
	}
	cancel() // 停机中断：重试等待的唯一就绪分支是 ctx.Done
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processTask did not return")
	}

	// 不变量：任务必须回到可追踪的位置——回灌分片 ZSet（或进真死信），
	// 绝不允许三处皆空的静默丢弃。
	pending, err := b.PendingCount(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dead, err := b.client.ZCard(context.Background(), fmt.Sprintf(deadLetterKey, "steal")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending == 0 && dead == 0 {
		t.Fatalf("任务静默丢弃：分片 ZSet 与死信 ZSet 皆空（#151：重试等待中被中断后裸 return）")
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want 1（中断的 claim 须回灌 Redis）", pending)
	}
}

// TestProcessTask_StolenClaimCanceledCtx_NoFakeDeadLetter 钉住 #151 的「假死信」
// 半边：停机窗口内被抢走的 claim 用已取消的 ctx 跑 handler 必然立刻失败——
// 这不是真失败，不得把「从未真正处理」的任务写进死信 ZSet，也不得静默丢弃。
//
// 缺陷成因：processTask 不区分「真失败」与「停机打断」，重试耗尽即
// sendToDeadLetter——已取消 ctx 下重试全败是必然结果，死信成了假死信；
// MaxRetry=0 时连重试窗口都进不去，一次失败直落死信。
//
// 复现（确定性）：claim 后用已取消的 ctx 直接驱动 processTask（= 被抢任务
// 在回灌窗口内的处理形态），MaxRetry=0 一次失败即终态。
func TestProcessTask_StolenClaimCanceledCtx_NoFakeDeadLetter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		atLeast bool
	}{
		{"at-most-once", false},
		{"at-least-once", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []timertask.TimerTaskOption{
				timertask.WithRetry(0, time.Hour),
				timertask.WithBuckets(4),
			}
			if tc.atLeast {
				opts = append(opts, timertask.WithAtLeastOnce(30*time.Second))
			}
			b := newStealBackend(t, func(ctx context.Context, task *timertask.Task) error {
				// 已取消的 ctx：handler 立刻失败（被抢任务的真实处境）
				return errors.New("canceled ctx: " + ctx.Err().Error())
			}, opts...)

			c := claimDueTask(t, b, "steal-fake-1", 0)

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // 停机信号先于处理到达
			b.processTask(ctx, c)

			dead, err := b.client.ZCard(context.Background(), fmt.Sprintf(deadLetterKey, "steal")).Result()
			if err != nil {
				t.Fatal(err)
			}
			if dead != 0 {
				t.Fatalf("假死信：从未真正处理的任务被写进死信 ZSet（#151），dead = %d", dead)
			}
			pending, err := b.PendingCount(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			leased, err := b.client.ZCard(context.Background(), b.processingKey()).Result()
			if err != nil {
				t.Fatal(err)
			}
			if pending == 0 && leased == 0 {
				t.Fatal("任务静默丢弃：分片 ZSet 与 processing 租约集皆空（#151）")
			}
		})
	}
}
