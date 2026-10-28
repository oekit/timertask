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
