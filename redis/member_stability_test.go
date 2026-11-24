package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// #188 回归测试：租约到期回投写回的 member 必须与 Send 产出**同一字节串**
// （「同拍重发 = 同 member」不变量要在任务经历过租约到期之后仍成立）。
//
// 缺陷成因：#76 把 member 规范化收敛到 Send（钉 CreateTime=EndTime、补
// MaxRetry 缺省），但 member 里还有第二个可变字段 retry_count——回投路径
// reapExpiredLeases 自己 `task.RetryCount++` 后直接 json.Marshal 写回，不走
// Send 的规范化 ⇒ 回投 member 与 Send member 只差 retry_count 一个字段，而
// ZSet 的去重键就是整个 member 字节串 ⇒ 生产者 at-least-once 重发同一任务时
// 同一 (task_id, end_time) 出现两条条目，业务 handler 双跑。
//
// 修复口径：member 归零携带 retry_count（canonicalMember 单源，Send 与回投
// 两条写入面共用），真实计数单源在 retry-count hash（retryCountKey）。
func TestLeaseRequeue_MemberStableAcrossResend(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	ctx := context.Background()
	task := &timertask.Task{
		ID:       "dup-1",
		EndTime:  time.Now().Unix() - 10,
		Action:   "act",
		Param:    `{"x":1}`,
		MaxRetry: 3,
	}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}
	key := b.taskKey(task.ID)
	members, err := b.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("Send 后分片条目数 = %d err=%v, want 1", len(members), err)
	}
	sendMember := members[0]

	// claim 到 processing（与 consumeRemoteOnce 同一原子面）
	now := b.now().Unix()
	ok, err := b.claimTaskWithLease(ctx, key, sendMember, now)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v, want true/nil", ok, err)
	}

	// 租约到期（确定性模拟：把租约 score 拨到过去，等价时间流逝过 visibility）
	if err := b.client.ZAdd(ctx, b.processingKey(), goredis.Z{
		Score:  float64(now - 100),
		Member: sendMember,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	b.reapExpiredLeases(ctx)

	// 回投后：分片 1 条、processing 0 条
	requeued, err := b.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil || len(requeued) != 1 {
		t.Fatalf("回投后分片条目数 = %d err=%v, want 1", len(requeued), err)
	}
	proc, err := b.client.ZCard(ctx, b.processingKey()).Result()
	if err != nil || proc != 0 {
		t.Fatalf("回投后 processing 条目数 = %d err=%v, want 0", proc, err)
	}

	// 核心断言：回投 member == Send member（字节级一致）。
	// 修复前两者差 retry_count 一个字段，是不同条目。
	if requeued[0] != sendMember {
		t.Fatalf("回投 member != Send member（#188 不变量被打破）:\n  send    = %s\n  requeue = %s",
			sendMember, requeued[0])
	}

	// 同拍重发同一任务：命中 ZAdd 的 score 更新而非新增条目
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}
	after, err := b.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("经历租约到期后同拍重发出现 %d 条条目（#188：同 (task_id, end_time) 必须仍只有 1 条）: %v",
			len(after), after)
	}
	if after[0] != sendMember {
		t.Fatalf("重发后 member 被改写:\n  expect = %s\n  got    = %s", sendMember, after[0])
	}
}

// #188 伴随断言：重试计数单源在 retry-count hash——回投一轮字段 +1；
// worker 侧以 hash 累计值为基线（RunWithRetry 在其上推进，#189 口径），
// 死信记录携带真实累计值；终态 Ack 顺手清理 hash 字段。
func TestLeaseRequeue_RetryCountHashLifecycle(t *testing.T) {
	b, _ := newTestRedisBackend(t)
	b.delivery = timertask.DeliveryAtLeastOnce
	b.visibilityTimeout = 2 * time.Second
	b.handler = func(context.Context, *timertask.Task) error {
		return context.DeadlineExceeded // 恒失败，走耗尽路径
	}
	ctx := context.Background()
	task := &timertask.Task{
		ID:       "cnt-1",
		EndTime:  time.Now().Unix() - 10,
		Action:   "act",
		MaxRetry: 1,
	}
	if err := b.Send(ctx, task); err != nil {
		t.Fatal(err)
	}
	key := b.taskKey(task.ID)
	members, err := b.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("Send 后分片条目数 = %d err=%v, want 1", len(members), err)
	}
	sendMember := members[0]

	// 回投一轮（claim → 租约到期 → reap）
	now := b.now().Unix()
	if ok, err := b.claimTaskWithLease(ctx, key, sendMember, now); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := b.client.ZAdd(ctx, b.processingKey(), goredis.Z{
		Score:  float64(now - 100),
		Member: sendMember,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	b.reapExpiredLeases(ctx)

	// hash 计数 = 1（一轮租约到期）
	field := b.retryCountField(task)
	got, err := b.client.HGet(ctx, b.retryCountKey(), field).Int64()
	if err != nil || got != 1 {
		t.Fatalf("retry-count hash = %d err=%v, want 1", got, err)
	}

	// worker 消费（重投 member 解析出 RetryCount=0，基线取 hash 累计值 1）：
	// MaxRetry=1 ⇒ 本轮 2 次尝试，耗尽后死信记录应带累计值 2（#189 少报口径）。
	requeued, err := b.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil || len(requeued) != 1 {
		t.Fatalf("回投后分片条目 = %d err=%v", len(requeued), err)
	}
	parsed := b.parseTask(requeued[0])
	if parsed == nil {
		t.Fatal("回投 member 不可解析")
	}
	b.processTask(ctx, &claim{task: parsed, member: requeued[0]})

	// 死信记录：retry_count 必须是累计值 2（1 轮租约回投 + 本轮 1 次重试），
	// 修复前死信记录只写循环末序号 1。
	dead, err := b.client.ZRange(ctx, fmt.Sprintf(deadLetterKey, b.buzType), 0, -1).Result()
	if err != nil || len(dead) != 1 {
		t.Fatalf("死信条目数 = %d err=%v, want 1", len(dead), err)
	}
	var recorded timertask.Task
	if err := json.Unmarshal([]byte(dead[0]), &recorded); err != nil {
		t.Fatal(err)
	}
	if recorded.RetryCount != 2 {
		t.Fatalf("死信记录 retry_count = %d, want 2（租约回投 1 + 本轮 1；#189：死信不得少报）", recorded.RetryCount)
	}

	// 终态 Ack 清理 hash 字段（不留孤儿计数）
	exists, err := b.client.HExists(ctx, b.retryCountKey(), field).Result()
	if err != nil || exists {
		t.Fatalf("终态后 retry-count hash 字段仍存在 = %v err=%v, want 清理", exists, err)
	}
	// processing 已摘除
	proc, err := b.client.ZCard(ctx, b.processingKey()).Result()
	if err != nil || proc != 0 {
		t.Fatalf("终态后 processing = %d err=%v, want 0", proc, err)
	}
}

// TestRequeueClaim_MemberStableWithRewrittenTask #279 回归测试：停机回投
// （requeueClaim）产出的 member 必须与 Send 产出**同一字节串**——即使 claim
// 携带的 task 指针已被中断路径改写 RetryCount。
//
// 缺陷成因：requeueClaim 用裸 json.Marshal(task)，而该 task 指针在
// processTask 里被改写（retry-count hash 基线回写、RunWithRetry 重试推进，
// redis.go:642-644 / retry.go:99），停机中断 → requeueClaim 时 marshal 出的
// member 携带非零 retry_count，与 Send 的 canonical member 字节分叉 ⇒
// at-most-once 下 ZAdd 变新增条目而非 score 更新，业务同拍幂等重发（Send）
// 即两条 member 双跑——#76/#188 钉死的「同拍重发 = 同 member」不变量在第三
// 写入面失效。
//
// 修复口径：requeueClaim 的两处 member 序列化（at-most-once 的 ZAdd member
// 与 at-least-once 脚本 ARGV[2]）统一走 canonicalMember（#188 单源收编第三
// 写入面）；canonicalMember 对已 canonical 的 task 幂等。
func TestRequeueClaim_MemberStableWithRewrittenTask(t *testing.T) {
	for _, tc := range []struct {
		name    string
		atLeast bool
	}{
		{"at-most-once", false},
		{"at-least-once", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newTestRedisBackend(t)
			if tc.atLeast {
				b.delivery = timertask.DeliveryAtLeastOnce
			}
			ctx := context.Background()
			task := &timertask.Task{
				ID:       "rq-1",
				EndTime:  time.Now().Unix() - 10,
				Action:   "act",
				Param:    `{"x":1}`,
				MaxRetry: 3,
			}
			if err := b.Send(ctx, task); err != nil {
				t.Fatal(err)
			}
			key := b.taskKey(task.ID)
			members, err := b.client.ZRange(ctx, key, 0, -1).Result()
			if err != nil || len(members) != 1 {
				t.Fatalf("Send 后分片条目数 = %d err=%v, want 1", len(members), err)
			}
			sendMember := members[0]

			// 模拟中断路径的改写：processTask 把 retry-count hash 基线 /
			// RunWithRetry 的推进写回 task 指针后停机中断，回灌的就是这份
			// 已被改写的 task（RetryCount ≠ 0）。
			rewritten := *task
			rewritten.RetryCount = 2
			if !b.requeueClaim(ctx, &claim{task: &rewritten, member: sendMember}) {
				t.Fatal("requeueClaim 失败")
			}

			// 回投后分片必须仍只有 1 条且字节 == Send member：修复前裸
			// marshal 产出携带 retry_count=2 的分叉字节串，ZAdd 新增条目
			// （at-most-once）或脚本回投分叉 member（at-least-once）。
			after, err := b.client.ZRange(ctx, key, 0, -1).Result()
			if err != nil || len(after) != 1 {
				t.Fatalf("回投后分片条目数 = %d err=%v, want 1（双 member 即双跑）", len(after), err)
			}
			if after[0] != sendMember {
				t.Fatalf("回投 member != Send member（#279 不变量被打破）:\n  send    = %s\n  requeue = %s",
					sendMember, after[0])
			}
			// 直接钉单源：requeueClaim 的 member 序列化必须与 canonicalMember
			// 产出同一字节串。
			canonical, err := b.canonicalMember(&rewritten)
			if err != nil {
				t.Fatal(err)
			}
			if after[0] != canonical {
				t.Fatalf("回投 member != canonicalMember 产出:\n  canonical = %s\n  requeue   = %s",
					canonical, after[0])
			}
		})
	}
}
