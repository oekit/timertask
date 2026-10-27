package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// TestReapExpiredLeases_NegativeMaxRetry_NotInfiniteRequeue 回归测试：
// MaxRetry 为负（「不重试」）的任务，租约到期后必须**进死信**，而不是被无限
// 重投。
//
// 缺陷成因：回扫的「重试次数耗尽」判定带了 `&& task.MaxRetry > 0` 守卫 ⇒
// MaxRetry<=0 时该分支**永远不可达** ⇒ 任务被无限重投（既不进死信、也不
// 停止）；而 worker 侧 processTask 对同一份任务早已归一为「执行一次、不
// 重试」⇒ 同一份任务在两条路径上的语义分叉。
//
// 修法：回扫侧先归一负数（与 worker 侧同口径），并去掉那个守卫。
func TestReapExpiredLeases_NegativeMaxRetry_NotInfiniteRequeue(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	b := NewRedisBackend("rel", client, func(ctx context.Context, task *timertask.Task) error {
		return nil
	}, timertask.WithAtLeastOnce(1*time.Second))

	// 造一份 MaxRetry=-1 的任务，直接放进 processing 租约集（模拟 worker 崩溃
	// 后租约到期）。
	task := &timertask.Task{ID: "neg-1", EndTime: time.Now().Unix() - 10, MaxRetry: -1}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	// 租约 score 是到期时刻（已过期 ⇒ score <= now）。
	if err := client.ZAdd(context.Background(), b.processingKey(), goredis.Z{
		Score:  float64(time.Now().Unix() - 100),
		Member: string(data),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	b.reapExpiredLeases(context.Background())

	// 租约必须已被摘掉（不再无限重投）。
	n, err := client.ZCard(context.Background(), b.processingKey()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("租约集仍有 %d 条：MaxRetry=-1 的任务被无限重投", n)
	}
	// 且必须进了死信（而不是被静默丢弃）。
	dead, err := client.ZCard(context.Background(), fmt.Sprintf(deadLetterKey, "rel")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if dead == 0 {
		t.Fatal("MaxRetry=-1 的任务既没进死信也没被释放：租约回扫的「耗尽」分支不可达")
	}
}

// TestReapExpiredLeases_PositiveMaxRetry_StillRequeues 回归护栏：MaxRetry>0
// 时，租约到期仍按次数重投（既有口径不变）。
func TestReapExpiredLeases_PositiveMaxRetry_StillRequeues(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	b := NewRedisBackend("rel", client, func(ctx context.Context, task *timertask.Task) error {
		return nil
	}, timertask.WithAtLeastOnce(1*time.Second))

	task := &timertask.Task{ID: "pos-3", EndTime: time.Now().Unix() - 10, MaxRetry: 3}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), b.processingKey(), goredis.Z{
		Score:  float64(time.Now().Unix() - 100),
		Member: string(data),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	b.reapExpiredLeases(context.Background())

	// 租约被摘掉、任务回到分片 ZSet（重投）。
	n, err := client.ZCard(context.Background(), b.processingKey()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("租约集仍有 %d 条", n)
	}
	requeued, err := client.ZCard(context.Background(), b.taskKey("pos-3")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if requeued == 0 {
		t.Fatal("MaxRetry=3 的任务未被重投回分片 ZSet")
	}
}
