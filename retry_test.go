package timertask

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRunWithRetry_Outcomes 钉住重试主循环单源（#169）的三种退出形态与
// 钩子契约——两后端（local/redis）共用本骨架，行为在此单点钉死。
func TestRunWithRetry_Outcomes(t *testing.T) {
	t.Run("success after retries", func(t *testing.T) {
		task := &Task{ID: "t1", MaxRetry: 3}
		calls := 0
		var retries []int
		outcome, err := RunWithRetry(context.Background(), task,
			func(context.Context, *Task) error {
				calls++
				if calls < 3 {
					return errors.New("flaky")
				}
				return nil
			}, time.Millisecond, nil, RetryHooks{
				OnRetry: func(attempt, maxRetry int) { retries = append(retries, attempt) },
			})
		if outcome != RetrySuccess || err != nil {
			t.Fatalf("outcome=%v err=%v, want RetrySuccess/nil", outcome, err)
		}
		if calls != 3 || task.RetryCount != 2 {
			t.Fatalf("calls=%d RetryCount=%d, want 3/2", calls, task.RetryCount)
		}
		if len(retries) != 2 || retries[0] != 1 || retries[1] != 2 {
			t.Fatalf("OnRetry attempts=%v, want [1 2]", retries)
		}
	})

	t.Run("exhausted returns last error", func(t *testing.T) {
		task := &Task{ID: "t2", MaxRetry: 2}
		last := errors.New("final failure")
		errCount := 0
		outcome, err := RunWithRetry(context.Background(), task,
			func(context.Context, *Task) error { return last },
			time.Millisecond, nil, RetryHooks{
				OnError: func(error) { errCount++ },
			})
		if outcome != RetryExhausted {
			t.Fatalf("outcome=%v, want RetryExhausted", outcome)
		}
		if !errors.Is(err, last) {
			t.Fatalf("err=%v, want 最后一次 handler 错误", err)
		}
		if errCount != 3 { // 1 次首发 + 2 次重试，每次失败都回调
			t.Fatalf("OnError 次数=%d, want 3", errCount)
		}
		if task.RetryCount != 2 {
			t.Fatalf("RetryCount=%d, want 2", task.RetryCount)
		}
	})

	t.Run("interrupted via stop signal", func(t *testing.T) {
		task := &Task{ID: "t3", MaxRetry: 5}
		stop := make(chan struct{})
		close(stop) // 已停机：首次重试的退避等待立即命中 stop
		interrupted := 0
		boom := errors.New("boom")
		outcome, err := RunWithRetry(context.Background(), task,
			func(context.Context, *Task) error { return boom },
			time.Hour, stop, RetryHooks{
				OnInterrupted: func() { interrupted++ },
			})
		if outcome != RetryInterrupted {
			t.Fatalf("outcome=%v, want RetryInterrupted", outcome)
		}
		if !errors.Is(err, boom) {
			t.Fatalf("err=%v, want 打断前的最后错误", err)
		}
		if interrupted != 1 || task.RetryCount != 0 {
			// 打断发生在第一次退避等待，RetryCount 尚未推进
			t.Fatalf("OnInterrupted=%d RetryCount=%d, want 1/0", interrupted, task.RetryCount)
		}
	})

	t.Run("interrupted via hook (redis 形态)", func(t *testing.T) {
		task := &Task{ID: "t4", MaxRetry: 5}
		hooked := 0
		outcome, _ := RunWithRetry(context.Background(), task,
			func(context.Context, *Task) error { return errors.New("boom") },
			time.Millisecond, nil, RetryHooks{
				Interrupted:   func() bool { return true },
				OnInterrupted: func() { hooked++ },
			})
		if outcome != RetryInterrupted || hooked != 1 {
			t.Fatalf("outcome=%v hooked=%d, want RetryInterrupted/1", outcome, hooked)
		}
	})

	t.Run("negative MaxRetry normalized", func(t *testing.T) {
		task := &Task{ID: "t5", MaxRetry: -5}
		calls := 0
		outcome, _ := RunWithRetry(context.Background(), task,
			func(context.Context, *Task) error { calls++; return errors.New("boom") },
			time.Millisecond, nil, RetryHooks{})
		if outcome != RetryExhausted || calls != 1 || task.MaxRetry != 0 {
			t.Fatalf("outcome=%v calls=%d MaxRetry=%d, want RetryExhausted/1/0（负值归一）",
				outcome, calls, task.MaxRetry)
		}
	})
}

// TestWithRetry_NegativeValues_Panic 钉住 #176：WithRetry 构造期校验——负
// count / 负 delay 直接 panic（对齐「配置错误启动即暴露」纪律），0 值合法。
// 此前负 count 被下游 NormalizeRetry 隐式归 0（配置错误静默变义），负 delay
// 让重试 timer 立即触发、MaxRetry 次忙重试。
func TestWithRetry_NegativeValues_Panic(t *testing.T) {
	requirePanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: 非法值应 panic（#176 构造期校验）", name)
			}
		}()
		fn()
	}
	requirePanic("negative count", func() { WithRetry(-1, time.Second) })
	requirePanic("negative delay", func() { WithRetry(1, -time.Second) })

	// 0 值合法：显式不重试 / 显式零退避
	cfg := applyOptions([]TimerTaskOption{WithRetry(0, 0)})
	if cfg.retryCount != 0 || cfg.retryDelay != 0 {
		t.Fatalf("0 值应原样生效: retryCount=%d retryDelay=%v", cfg.retryCount, cfg.retryDelay)
	}
}

// TestRunWithRetry_RetryCountCumulative 钉住 #189：RetryCount 是「已重试次数」
// 的跨调用累计量，RunWithRetry 必须在入参基础上**推进**而不是用进程内尝试
// 序号覆盖——入参携带租约回投累计值（redis 侧 task.RetryCount++，#188 后单源
// 在 retry-count hash）时，handler 看到的序列单调不回退；耗尽退出后
// task.RetryCount = base + MaxRetry（死信记录不再少报）。
func TestRunWithRetry_RetryCountCumulative(t *testing.T) {
	t.Run("入参累计值 3 时序列单调不回退", func(t *testing.T) {
		task := &Task{ID: "cum-1", RetryCount: 3, MaxRetry: 3}
		var seen []int
		outcome, _ := RunWithRetry(context.Background(), task,
			func(_ context.Context, task *Task) error {
				seen = append(seen, task.RetryCount)
				return errors.New("always fail")
			}, time.Millisecond, nil, RetryHooks{})
		if outcome != RetryExhausted {
			t.Fatalf("outcome=%v, want RetryExhausted", outcome)
		}
		// 修复前实测 [3 1 2 3]：第 2 次尝试看到 1，比第 1 次的 3 小——回退。
		for i := 1; i < len(seen); i++ {
			if seen[i] < seen[i-1] {
				t.Fatalf("RetryCount 序列回退: %v（#189：已重试次数必须单调不回退）", seen)
			}
		}
		want := []int{3, 4, 5, 6}
		if len(seen) != len(want) {
			t.Fatalf("handler 看到序列 %v, want %v", seen, want)
		}
		for i := range want {
			if seen[i] != want[i] {
				t.Fatalf("handler 看到序列 %v, want %v", seen, want)
			}
		}
		if task.RetryCount != 6 {
			t.Fatalf("退出后 task.RetryCount=%d, want 6（累计值不被抹掉）", task.RetryCount)
		}
	})

	t.Run("对照组：入参 0 时序列不变 [0 1 2 3]", func(t *testing.T) {
		task := &Task{ID: "cum-0", MaxRetry: 3}
		var seen []int
		RunWithRetry(context.Background(), task,
			func(_ context.Context, task *Task) error {
				seen = append(seen, task.RetryCount)
				return errors.New("always fail")
			}, time.Millisecond, nil, RetryHooks{})
		want := []int{0, 1, 2, 3}
		if len(seen) != len(want) {
			t.Fatalf("handler 看到序列 %v, want %v", seen, want)
		}
		for i := range want {
			if seen[i] != want[i] {
				t.Fatalf("handler 看到序列 %v, want %v（入参 0 的既有口径不得变化）", seen, want)
			}
		}
	})
}
