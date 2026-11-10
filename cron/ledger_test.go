package cron

import (
	"context"
	"testing"
	"time"
)

// TestMemLedger_BoundedRing 有界环形：超容覆盖最旧，Recent 时间序旧→新。
func TestMemLedger_BoundedRing(t *testing.T) {
	m := NewMemLedger(2)
	for i := range 3 {
		m.Record(context.Background(), RunRecord{PlanID: "p", Trigger: string(rune('a' + i))})
	}
	got := m.Recent()
	if len(got) != 2 {
		t.Fatalf("容量 2 的台账应只剩 2 条, got %d", len(got))
	}
	if got[0].Trigger != "b" || got[1].Trigger != "c" {
		t.Fatalf("应覆盖最旧并保持时间序, got %q %q", got[0].Trigger, got[1].Trigger)
	}
}

// TestMemLedger_RecentFor 按计划过滤。
func TestMemLedger_RecentFor(t *testing.T) {
	m := NewMemLedger(0) // 0 → DefaultRunCapacity
	m.Record(context.Background(), RunRecord{PlanID: "p1", Trigger: "t1"})
	m.Record(context.Background(), RunRecord{PlanID: "p2", Trigger: "t2"})
	m.Record(context.Background(), RunRecord{PlanID: "p1", Trigger: "t3"})
	got := m.RecentFor("p1")
	if len(got) != 2 || got[0].Trigger != "t1" || got[1].Trigger != "t3" {
		t.Fatalf("RecentFor(p1) 不符: %+v", got)
	}
	if all := m.Recent(); len(all) != 3 {
		t.Fatalf("Recent 应含全部 3 条, got %d", len(all))
	}
}

// TestMemLedger_RecordPanicSafe 台账自身的 Record 契约：无阻塞无 panic
// （Scheduler.record 外层另有 panic 收编——台账故障不得打死触发）。
func TestMemLedger_RecordPanicSafe(t *testing.T) {
	m := NewMemLedger(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			m.Record(context.Background(), RunRecord{PlanID: "p"})
			_ = m.Recent()
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record/Recent 并发下不应阻塞")
	}
}
