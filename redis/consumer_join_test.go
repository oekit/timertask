package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// blockingScanClient 把首次 ZRangeByScore（consumeRemoteOnce 的到期扫描）
// 卡住，直到 release 关闭——测试借此把「消费者仍在飞」与「Stop 已进入停机
// 序列」重叠，确定性断言 join 顺序（#292）。
type blockingScanClient struct {
	goredis.UniversalClient
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingScanClient) ZRangeByScore(ctx context.Context, key string, opt *goredis.ZRangeBy) *goredis.StringSliceCmd {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.UniversalClient.ZRangeByScore(ctx, key, opt)
}

// TestStart_JoinsConsumerBeforeDrain 钉住 #292 的停机序：
// close(stopChan) → join 消费者 → drain worker → 回灌。
//
// 旧实现只等 workerWG，消费循环仍可能在飞：consumeRemoteOnce 在 claim
// （分片 ZSet 原子删除 / 移入租约集）之后、入队之前遇停机时，localQueue
// 与已关闭 stopChan 同时就绪，Go select 随机取 localQueue——迟到入队的
// claim 落在回灌排水之后，无人消费（at-most-once 永久丢任务）。
//
// 本测试卡住消费者的扫描调用模拟「仍在飞的消费者」：Start 必须等它退出
// （join）才能返回；修复前 Start 会在 join 之前就返回。
func TestStart_JoinsConsumerBeforeDrain(t *testing.T) {
	mr := miniredis.RunT(t)
	base := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = base.Close() })

	client := &blockingScanClient{
		UniversalClient: base,
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	b := NewRedisBackend("joinscan", client, func(context.Context, *timertask.Task) error {
		return nil
	}, timertask.WithWorkers(1), timertask.WithBuckets(4))

	ctx, cancel := context.WithCancel(context.Background())
	startErr := make(chan error, 1)
	go func() { startErr <- b.Start(ctx) }()

	// 等消费者真正进入到期扫描（之后它会阻塞在 blockingScanClient 里）。
	select {
	case <-client.entered:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("consumeRemoteLoop 未进入扫描（正对照失败）")
	}
	cancel()

	// 消费者仍在飞：Start 不得越过 join 返回。
	select {
	case err := <-startErr:
		t.Fatalf("Start 在消费者退出前返回（%v）：停机序缺 join，迟到入队会丢任务（#292）", err)
	case <-time.After(200 * time.Millisecond):
	}

	// 放行消费者：join 完成后 Start 才能返回。
	close(client.release)
	select {
	case err := <-startErr:
		if err != context.Canceled {
			t.Fatalf("Start = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("消费者放行后 Start 仍未返回")
	}
}
