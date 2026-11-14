package cron

import (
	"testing"
	"time"
)

// TestRetryBudget_LockTTLFormula 钉住锁 TTL 预算公式的单一出处（#159）：
// 锁 TTL = 全部尝试 + 全部退避 + 余量 = Attempts*Timeout + TotalBackoff() + Slack。
// 此前该公式散在下游手算（zeus/task execLockTTL），口径漂移 ⇒ 互斥提前
// 过期 ⇒ 同 slot 双跑——必须只有一个地方定义。
func TestRetryBudget_LockTTLFormula(t *testing.T) {
	b := RetryBudget{
		Attempts: 3,
		Timeout:  60 * time.Second,
		Backoff:  5 * time.Second,
		Slack:    30 * time.Second,
	}
	if got, want := b.TotalBackoff(), 10*time.Second; got != want {
		t.Fatalf("TotalBackoff = %v, want %v (2 个退避间隔)", got, want)
	}
	if got, want := b.LockTTL(), 220*time.Second; got != want {
		t.Fatalf("LockTTL = %v, want %v (3*60 + 2*5 + 30)", got, want)
	}
}

// TestRetryBudget_Normalized 归一语义：负值/零值钳制（min/max 内建），
// Slack 零值取默认、显式更小的正值照用。
func TestRetryBudget_Normalized(t *testing.T) {
	z := RetryBudget{}.Normalized()
	// timertask#244：零值 Timeout 改取缺省（此前归 0 = 无时限，与 LockTTL
	// 租约公式矛盾致双跑）——归一语义随契约更新。
	if z.Attempts != 1 || z.Timeout != DefaultBudgetTimeout || z.Backoff != 0 || z.Slack != DefaultBudgetSlack {
		t.Fatalf("零值归一不符: %+v", z)
	}
	if got := (RetryBudget{Attempts: -5}).Normalized().Attempts; got != 1 {
		t.Fatalf("负 Attempts 应归 1, got %d", got)
	}
	if got := (RetryBudget{Slack: time.Second}).Normalized().Slack; got != time.Second {
		t.Fatalf("显式更小的 Slack 应照用, got %v", got)
	}
	// 零值预算的 LockTTL = 1×DefaultBudgetTimeout + 0 + Slack（#244：
	// 租约必须覆盖执行时限，不再只剩 Slack）。
	if got, want := (RetryBudget{}).LockTTL(), DefaultBudgetTimeout+DefaultBudgetSlack; got != want {
		t.Fatalf("零值 LockTTL = %v, want %v", got, want)
	}
}

// timertask#244：零值 Timeout = 缺省——此前 "<=0 归 0 = 无时限" 让
// WithRetryBudget(RetryBudget{Attempts:5}) 的 LockTTL 只剩 Slack(30s)，
// 任务跑过租约即过期、他实例接管双跑（击穿预算公式的立项理由）。
func TestRetryBudgetZeroValueTimeoutCoherent(t *testing.T) {
	b := RetryBudget{Attempts: 5}
	if ttl := b.LockTTL(); ttl < 5*DefaultBudgetTimeout {
		t.Fatalf("LockTTL = %v, want >= %v(零值 Timeout 取缺省,#244)",
			ttl, 5*DefaultBudgetTimeout)
	}
	if ttl := (RetryBudget{}).LockTTL(); ttl <= 0 {
		t.Fatalf("全零值 LockTTL 必须为正, got %v", ttl)
	}
	// 显式 Timeout 照用（不被缺省覆盖）
	explicit := RetryBudget{Attempts: 2, Timeout: 5 * time.Second}
	if got := explicit.Normalized().Timeout; got != 5*time.Second {
		t.Fatalf("显式 Timeout 被覆盖: %v", got)
	}
}
