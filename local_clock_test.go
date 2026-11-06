package timertask

import (
	"context"
	"testing"
	"time"
)

// TestLocalBackend_ClockInjection 钉住 #78：LocalBackend 时间判断走注入时钟——
// FakeClock 把 CreateTime 与扫描「当前时间」拨到未来，任务不真睡即可入队不触发。
func TestLocalBackend_ClockInjection(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fake := &fakeClock78{now: now}
	b := NewLocalBackend(func(ctx context.Context, task *Task) error { return nil },
		WithClock(fake))
	if err := b.Send(context.Background(), &Task{ID: "t1", EndTime: now.Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.taskMap["t1"]; !ok {
		t.Fatal("task should be queued")
	}
	// 推进时钟到触发点之后，扫描应触发（不真睡）
	fake.now = now.Add(2 * time.Hour)
	b.scanOnce(context.Background())
	select {
	case task := <-b.localQueue:
		if task.ID != "t1" {
			t.Fatalf("task %q", task.ID)
		}
	default:
		t.Fatal("时钟推进后扫描应触发任务（#78：时钟注入使到点场景可测）")
	}
}

type fakeClock78 struct{ now time.Time }

func (f *fakeClock78) Now() time.Time { return f.now }
