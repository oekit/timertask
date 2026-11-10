package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// pipelineFlakyClient 是注入「pipeline 执行到一半遇到瞬时错误」的客户端替身：
// 透传全部真实调用（miniredis），只对 Pipeline().Exec 做手脚。
type pipelineFlakyClient struct {
	goredis.UniversalClient
}

func (c *pipelineFlakyClient) Pipeline() goredis.Pipeliner {
	return &transientFailPipeliner{
		Pipeliner: c.UniversalClient.Pipeline(),
		real:      c.UniversalClient,
	}
}

// transientFailPipeliner 模拟重投窗口内的部分失败：已入队命令中除最后一条外
// 都已在服务端真实执行，最后一条因瞬时错误（连接闪断 / CLUSTERDOWN / MOVED）
// 失败——这正是「ZRem 已执行、ZAdd 失败」这类半失败窗口的确定性模型。
type transientFailPipeliner struct {
	goredis.Pipeliner
	real goredis.UniversalClient
	ops  []func(ctx context.Context) error
}

func (p *transientFailPipeliner) ZAdd(ctx context.Context, key string, members ...goredis.Z) *goredis.IntCmd {
	k, m := key, members
	p.ops = append(p.ops, func(ctx context.Context) error {
		return p.real.ZAdd(ctx, k, m...).Err()
	})
	return goredis.NewIntCmd(ctx)
}

func (p *transientFailPipeliner) ZRem(ctx context.Context, key string, members ...interface{}) *goredis.IntCmd {
	k, m := key, members
	p.ops = append(p.ops, func(ctx context.Context) error {
		return p.real.ZRem(ctx, k, m...).Err()
	})
	return goredis.NewIntCmd(ctx)
}

func (p *transientFailPipeliner) Exec(ctx context.Context) ([]goredis.Cmder, error) {
	for i := 0; i < len(p.ops)-1; i++ {
		if err := p.ops[i](ctx); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("injected transient error during requeue")
}

// TestReapExpiredLeases_TransientErrorDuringRequeue_MustNotLoseTask 钉住 #150：
// reapExpiredLeases 的重投在 pipeline 中途遇到瞬时错误时，任务不得永久丢失。
//
// 缺陷成因：重投用「先摘租约（ZRem）再回投（ZAdd）」的非原子 pipeline——
// ZRem 已执行而 ZAdd 因瞬时错误失败时，任务从 processing 消失且不再回到分片
// ZSet，哪都不在 ⇒ 永久丢失（不进死信、回扫也捞不回），与 at-least-once
// 「租约超时由回扫重投、不会丢」的承诺相悖。对照 requeueClaim 是「先 ZAdd
// 再 ZRem」——失败退化为重复而非丢失。
//
// 复现（确定性，无 sleep、无竞态）：注入 pipeline 替身，让「除最后一条命令
// 外全部真实执行、最后一条失败」——修复前 pipeline 顺序是 ZRem→ZAdd，
// ZRem 成功 + ZAdd 失败 = 逐字复现丢任务窗口。
func TestReapExpiredLeases_TransientErrorDuringRequeue_MustNotLoseTask(t *testing.T) {
	mr := miniredis.RunT(t)
	real := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = real.Close() })

	b := NewRedisBackend("reaporder", real, func(ctx context.Context, task *timertask.Task) error {
		return nil
	}, timertask.WithAtLeastOnce(30*time.Second))

	// 模拟崩溃实例留下已过期未 Ack 的租约（at-least-once 的回扫入口形态）。
	task := &timertask.Task{ID: "lost-1", EndTime: time.Now().Unix() - 10, MaxRetry: 3}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err = real.ZAdd(context.Background(), b.processingKey(), goredis.Z{
		Score:  float64(time.Now().Unix() - 100),
		Member: string(data),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	// 重投期间注入一次瞬时错误。
	b.client = &pipelineFlakyClient{UniversalClient: real}
	b.reapExpiredLeases(context.Background())

	// at-least-once 不变量：任务必须仍可追踪——要么已回投分片 ZSet，
	// 要么租约还在 processing 等下轮回扫；绝不允许两处皆空。
	ctx := context.Background()
	shard, err := real.ZCard(ctx, b.taskKey("lost-1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	proc, err := real.ZCard(ctx, b.processingKey()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if shard == 0 && proc == 0 {
		t.Fatalf("任务永久丢失：分片 ZSet 与 processing 租约集皆空（#150：重投 pipeline 半失败窗口）")
	}
	if shard > 0 {
		// 回投的 member 必须完好可解析
		members, err := real.ZRange(ctx, b.taskKey("lost-1"), 0, -1).Result()
		if err != nil || len(members) != 1 {
			t.Fatalf("回投 member 异常: %v (%d)", err, len(members))
		}
		if got := b.parseTask(members[0]); got == nil || got.ID != "lost-1" {
			t.Fatalf("回投 member 损坏: %+v", got)
		}
	}
}
