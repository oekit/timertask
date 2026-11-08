package redis

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oekit/timertask"
	goredis "github.com/redis/go-redis/v9"
)

// newBlackholeAddr 起一个「接受连接但永不响应」的 TCP 端点，模拟 Redis 黑洞
// （网络分区而非拒连）。此时客户端的写请求会「写成功但读不回响应」，
// 只有 ctx deadline 能把它救回来。
func newBlackholeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
			// 接受后既不读也不写 ⇒ 客户端永远等不到响应
		}
	}()
	return ln.Addr().String()
}

// newBlackholeClient 造一个指向黑洞的 redis 客户端。
//
// ReadTimeout 故意设得远大于 bestEffortTimeout ⇒ 「能救回这次调用」的
// 只可能是 ctx deadline（而非客户端自身的读超时）。
func newBlackholeClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	addr := newBlackholeAddr(t)
	client := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1, // 不重试 ⇒ 行为更贴近「单次调用挂住」
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// withShortBestEffort 把 bestEffortTimeout 调小（毫秒级复现黑洞场景）。
func withShortBestEffort(t *testing.T) {
	t.Helper()
	old := bestEffortTimeout
	bestEffortTimeout = 300 * time.Millisecond
	t.Cleanup(func() { bestEffortTimeout = old })
}

// withShortRequeueWindow 把停机回灌窗口调小（毫秒级复现黑洞场景）。
func withShortRequeueWindow(t *testing.T) {
	t.Helper()
	old := requeueShutdownWindow
	requeueShutdownWindow = 300 * time.Millisecond
	t.Cleanup(func() { requeueShutdownWindow = old })
}

// TestSendToDeadLetter_HasDeadline 回归测试：死信写入必须有 deadline。
//
// 缺陷成因：sendToDeadLetter 用 context.Background() 调 Redis ⇒ Redis 黑洞
// （网络分区而非拒连）时调用挂到 OS 级 TCP 超时（分钟级）⇒ worker goroutine
// 永不返回 ⇒ worker 池被拖垮、优雅停机卡住。
func TestSendToDeadLetter_HasDeadline(t *testing.T) {
	withShortBestEffort(t)
	client := newBlackholeClient(t)
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error { return nil })

	done := make(chan struct{})
	go func() {
		b.sendToDeadLetter(&timertask.Task{ID: "dead-1"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("死信写入未返回（Redis 黑洞时无 deadline ⇒ worker goroutine 永久卡住）")
	}
}

// TestProcessTask_TerminalAck_HasDeadline 回归测试：终态 Ack 租约必须有
// deadline（它在 defer 里，Redis 黑洞时同样会把 worker 挂住）。
func TestProcessTask_TerminalAck_HasDeadline(t *testing.T) {
	withShortBestEffort(t)
	client := newBlackholeClient(t)
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error { return nil },
		timertask.WithAtLeastOnce(30*time.Second))

	done := make(chan struct{})
	go func() {
		b.processTask(context.Background(), &claim{task: &timertask.Task{ID: "ack-1", MaxRetry: 0}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("终态 Ack 未返回（Redis 黑洞时无 deadline ⇒ worker goroutine 永久卡住）")
	}
}

// TestRequeuePendingOnShutdown_HasDeadline 回归测试 #121：停机回灌的「限时
// 窗口」对默认 go-redis 客户端是虚构的——ContextTimeoutEnabled 默认 false
// 时 go-redis 丢弃调用方 ctx deadline，回灌调用阻塞到客户端 ReadTimeout
// （本测试客户端 30s），「超窗按 at-most-once 放弃」永不发生，Start 退出
// 路径挂死。修复后每笔回灌套 bestEffortRedisTimeout（select + 弃子
// goroutine）实现真实墙钟窗口。
func TestRequeuePendingOnShutdown_HasDeadline(t *testing.T) {
	withShortRequeueWindow(t)
	client := newBlackholeClient(t)
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error { return nil })

	// localQueue 预置一条待回灌 claim（纯内存操作，无需 Redis 可用）
	task := &timertask.Task{ID: "rq-1", EndTime: time.Now().Unix()}
	b.localQueue <- &claim{task: task, member: mustJSON(task)}

	done := make(chan struct{})
	go func() {
		b.requeuePendingOnShutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("requeuePendingOnShutdown 未返回：回灌窗口对黑洞客户端无效（#121，裸 ctx deadline 被 go-redis 丢弃）")
	}
}

// TestRequeueClaimOnShutdown_HasDeadline 回归测试 #121：在途 claim 的停机
// 回灌（consumeRemoteOnce 的停机窗口）同样必须真实限时——探针实测 deadline
// ctx 下仍阻塞整段 ReadTimeout，错误信息还伪装成 context deadline exceeded。
func TestRequeueClaimOnShutdown_HasDeadline(t *testing.T) {
	withShortRequeueWindow(t)
	client := newBlackholeClient(t)
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error { return nil })

	task := &timertask.Task{ID: "rq-2", EndTime: time.Now().Unix()}
	done := make(chan struct{})
	go func() {
		b.requeueClaimOnShutdown(&claim{task: task, member: mustJSON(task)})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("requeueClaimOnShutdown 未返回：在途回灌窗口对黑洞客户端无效（#121）")
	}
}

// TestRequeueLeasesOnShutdown_HasDeadline 回归测试 #121：停机租约回扫的
// 内部是 ZRangeByScore + 逐 member 串行 pipeline，黑洞下 N 个租约会串行
// 阻塞 N×ReadTimeout 把 Start 退出路径挂死。修复后整轮回扫套
// bestEffortRedisTimeout 限时，超窗弃子、租约交回扫兜底。
func TestRequeueLeasesOnShutdown_HasDeadline(t *testing.T) {
	withShortRequeueWindow(t)
	client := newBlackholeClient(t)
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error { return nil },
		timertask.WithAtLeastOnce(30*time.Second))

	done := make(chan struct{})
	go func() {
		b.requeueLeasesOnShutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("requeueLeasesOnShutdown 未返回：回扫在黑洞上串行阻塞（#121）")
	}
}

// TestProcessTask_RenewJoin_BoundedOnBlackhole 回归测试 #122：at-least-once
// 模式下 worker 收尾要 join 续租 goroutine，续租调用（裸 ZAddArgs）阻塞在
// 黑洞里时 join 只能等客户端 ReadTimeout（本测试客户端 30s）解救——handler
// 早已返回，worker 却被一个与任务结果无关的续租写拖住。修复后续租调用自带
// 弃子限时 min(2s, interval)，join 至多等一个限时窗口。
//
// 复现编排（确定性）：handler 阻塞到首个续租 tick（visibilityTimeout=600ms
// ⇒ tick=200ms）已发出、renewLoop 正卡在续租写里，再放行 handler——旧实现
// 的收尾 join 卡到 ReadTimeout。
func TestProcessTask_RenewJoin_BoundedOnBlackhole(t *testing.T) {
	withShortBestEffort(t) // 收尾 Ack 同样限时，避免它的 5s 窗口污染计时
	client := newBlackholeClient(t)
	release := make(chan struct{})
	b := NewRedisBackend("blk", client, func(ctx context.Context, t *timertask.Task) error {
		<-release
		return nil
	}, timertask.WithAtLeastOnce(600*time.Millisecond))

	done := make(chan struct{})
	go func() {
		b.processTask(context.Background(), &claim{task: &timertask.Task{ID: "renew-1", MaxRetry: 0}})
		close(done)
	}()

	// 等首个续租 tick（200ms）发出并卡进续租写，再放行 handler
	time.Sleep(500 * time.Millisecond)
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processTask 未返回：收尾 join 被续租写卡住（#122），只能等 ReadTimeout 解救")
	}
}
