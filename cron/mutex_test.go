package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/timertask/local"
)

// TestLocalMutex_ConcurrentTryLock 互斥并发场景（进程内仲裁）：N 路并发
// 争抢同键，恰好一路获得。
func TestLocalMutex_ConcurrentTryLock(t *testing.T) {
	m := NewLocalMutex(nil)
	const n = 16
	var wins atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if owner, err := m.TryLock(context.Background(), "k", time.Minute); err == nil && owner != "" {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("恰好一路应获得锁, got %d", got)
	}
}

// TestLocalMutex_LeaseExpiry 租约语义（与仓内 at-least-once 租约同型）：
// TTL 到期自动可被接管——崩溃持有者不尸占。
func TestLocalMutex_LeaseExpiry(t *testing.T) {
	clock := local.NewFakeClock(time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))
	m := NewLocalMutex(clock)

	owner, err := m.TryLock(context.Background(), "k", 10*time.Second)
	if err != nil || owner == "" {
		t.Fatalf("首次获取应成功: owner=%q err=%v", owner, err)
	}
	if _, err := m.TryLock(context.Background(), "k", 10*time.Second); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("未到期应被占用, got %v", err)
	}

	clock.Advance(11 * time.Second) // 租约过期
	owner2, err := m.TryLock(context.Background(), "k", 10*time.Second)
	if err != nil || owner2 == "" {
		t.Fatalf("过期租约应可接管: owner=%q err=%v", owner2, err)
	}
	// 旧持有者事后 Unlock 不得删掉接管者的新租约（已易主报错）。
	if err := m.Unlock(context.Background(), "k", owner); err == nil {
		t.Fatal("陈旧持有者解锁应报错（已易主）")
	}
	if err := m.Unlock(context.Background(), "k", owner2); err != nil {
		t.Fatalf("接管者解锁应成功, got %v", err)
	}
}

// TestLocalMutex_UnlockSemantics Unlock 的 owner 匹配契约：不存在/不匹配
// 报错，匹配删除（幂等安全：重复解锁报错不误删）。
func TestLocalMutex_UnlockSemantics(t *testing.T) {
	m := NewLocalMutex(nil)
	owner, err := m.TryLock(context.Background(), "k", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if err := m.Unlock(context.Background(), "k", "not-"+owner); err == nil {
		t.Fatal("owner 不匹配应报错")
	}
	if err := m.Unlock(context.Background(), "k", owner); err != nil {
		t.Fatalf("匹配解锁应成功: %v", err)
	}
	if err := m.Unlock(context.Background(), "k", owner); err == nil {
		t.Fatal("重复解锁应报错（锁已不存在）")
	}
}
