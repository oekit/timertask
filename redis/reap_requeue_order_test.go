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
// 「租约超时由回扫重投、不会丢」的承诺相悖。对照 requeueClaim 的回投以
// ZAdd 先行——失败退化为重复而非丢失（#157 起两条路径统一走原子脚本）。
//
// 复现（确定性，无 sleep、无竞态）：注入 pipeline 替身，让「除最后一条命令
// 外全部真实执行、最后一条失败」——修复前 pipeline 顺序是 ZRem→ZAdd，
// ZRem 成功 + ZAdd 失败 = 逐字复现丢任务窗口。替身同时是「回退成流水线
// 重投」的护栏：#157 起重投走 requeueLeaseScript，本替身不再触发，一旦
// 有人把重投改回非原子 pipeline（尤其摘租约在前）本用例立即转红。
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

// TestReapExpiredLeases_RequeueWriteFails_LeaseMustStay 钉住 #157：重投时
// 回投写（ZAdd 回分片 ZSet）命令级失败（OOM / maxmemory 逐出 / 类型污染，
// 本用例以 WRONGTYPE 确定性模拟）后，摘租约（ZRem processing）不得执行——
// 「先回投成功再摘租约」。#150 只反转了同一条 pipeline 里的命令顺序：
// go-redis Pipeline() 非 MULTI/EXEC、逐条独立执行，「ZAdd 失败 + ZRem 成功」
// 的半失败腿仍然两处皆空、永久丢失，与 at-least-once「回扫重投、可容忍
// 重复、不容忍丢失」的承诺相悖。修复后重投收敛为 Lua 原子脚本
// （requeueLeaseScript）：redis.call 出错即中止，ZRem 不会执行 ⇒ 失败
// 退化为「租约保留、下轮回扫再投」。
//
// 复现（确定性，无注入客户端）：把分片 key 污染成 string ⇒ 回投的 ZAdd 必然
// WRONGTYPE 失败，而摘租约的 ZRem 在 processing（合法 zset）上照常成功——
// 逐字构造「回投失败 + 摘租约成功」的半失败窗口；再解除污染重跑回扫，
// 证明任务不丢是闭环（能被捡回重投），而不只是「暂留」。
func TestReapExpiredLeases_RequeueWriteFails_LeaseMustStay(t *testing.T) {
	mr := miniredis.RunT(t)
	real := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = real.Close() })

	b := NewRedisBackend("reap157", real, func(ctx context.Context, task *timertask.Task) error {
		return nil
	}, timertask.WithAtLeastOnce(30*time.Second))

	// 模拟崩溃实例留下已过期未 Ack 的租约（at-least-once 的回扫入口形态）。
	task := &timertask.Task{ID: "lost-157", EndTime: time.Now().Unix() - 10, MaxRetry: 3}
	member := mustJSON(task)
	if err := real.ZAdd(context.Background(), b.processingKey(), goredis.Z{
		Score:  float64(time.Now().Unix() - 100),
		Member: member,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	// 污染回投目标：回投的 ZAdd 必然命令级失败，摘租约的 ZRem 不受影响。
	if err := real.Set(context.Background(), b.taskKey("lost-157"), "poison", 0).Err(); err != nil {
		t.Fatal(err)
	}

	b.reapExpiredLeases(context.Background())

	// at-least-once 不变量（#157）：回投失败时租约必须保留——任务仍可被下轮
	// 回扫捡回，绝不允许「回投没进、租约被摘」的两处皆空。
	if _, err := real.ZScore(context.Background(), b.processingKey(), member).Result(); err != nil {
		t.Fatalf("#157 回归：回投写失败时租约被摘——任务将永久丢失（processing 已无此 member）: %v", err)
	}

	// 恢复腿：污染解除后重跑回扫 ⇒ 任务照常回投分片、租约随后摘除。
	if err := real.Del(context.Background(), b.taskKey("lost-157")).Err(); err != nil {
		t.Fatal(err)
	}
	b.reapExpiredLeases(context.Background())
	members, err := real.ZRange(context.Background(), b.taskKey("lost-157"), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("恢复后回投成员数 = %d，期望 1", len(members))
	}
	if got := b.parseTask(members[0]); got == nil || got.ID != "lost-157" {
		t.Fatalf("回投 member 损坏: %+v", got)
	}
	if n, err := real.ZCard(context.Background(), b.processingKey()).Result(); err != nil || n != 0 {
		t.Fatalf("回投成功后租约应已摘除：processing 剩 %d（err=%v）", n, err)
	}
}
