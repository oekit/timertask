package local

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collectFires 返回一个把触发时间发给带标记通道的任务。
func collectFires(mark chan<- string, mark_ string) Task {
	return func(_ context.Context, at time.Time) error {
		mark <- fmt.Sprintf("%s@%g", mark_, at.Sub(testStart).Seconds())
		return nil
	}
}

// waitMark 等待一个标记（最多 timeout），返回是否等到。
func waitMark(t *testing.T, mark <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case m := <-mark:
		return m
	case <-time.After(timeout):
		t.Fatalf("2s 内未收到触发标记（通道容量不足或任务未触发）")
		return ""
	}
}

// drainMark 非阻塞清空标记通道。
func drainMark(mark chan string) {
	for {
		select {
		case <-mark:
		default:
			return
		}
	}
}

// TestScheduleFor_TriggerSequence 钉住基础触发：业务键注册的任务按 cron
// 表达式依次触发，与 spec 键形态行为一致。
func TestScheduleFor_TriggerSequence(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 16)
	if err := s.ScheduleFor("task-1", "*/5 * * * * *", collectFires(mark, "t1")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	for i := 1; i <= 2; i++ {
		clock.Advance(5 * time.Second)
		got := waitMark(t, mark, 2*time.Second)
		if want := fmt.Sprintf("t1@%d", i*5); got != want {
			t.Errorf("step %d: 触发标记 = %s，期望 %s", i, got, want)
		}
	}
}

// TestScheduleFor_IdempotentSameSpec_NoEveryReset 回归 #129 的核心防呆：
// 同 key 同 spec 重复 upsert 是**幂等空操作**——不重装、不重置触发基准。
// @every 解析为 ConstantDelaySchedule（相对注册时刻的固定间隔）：若重复
// upsert 触发重装，lastFire 被重置到此刻，间隔长于重装周期的任务的下次
// 触发时间不断后移——永不执行（zeus task 指纹注释自文档化的坑）。
// 同时钉住「spec 未变时 Task 不换」：换任务函数请先 UnscheduleFor。
func TestScheduleFor_IdempotentSameSpec_NoEveryReset(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 16)
	if err := s.ScheduleFor("k", "@every 10s", collectFires(mark, "first")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	// 推进到触发点前 1s（首触发点 = T+10），此时重复 upsert 同 spec
	clock.Advance(9 * time.Second)
	if err := s.ScheduleFor("k", "@every 10s", collectFires(mark, "swapped")); err != nil {
		t.Fatalf("同 spec 重复 upsert 应幂等成功: %v", err)
	}

	// 若被重装，lastFire 会重置到 T+9 ⇒ 下次触发点变成 T+19，T+10 不触发
	clock.Advance(1 * time.Second)
	if got := waitMark(t, mark, 2*time.Second); got != "first@10" {
		t.Fatalf("#129 回归：同 spec 重复 upsert 后触发标记 = %q，期望 first@10（幂等空操作，不重装不重置）", got)
	}
	// 通道里不得有 swapped 的触发（Task 未被替换）
	select {
	case m := <-mark:
		t.Fatalf("spec 未变时 Task 不应被替换，收到 %q", m)
	default:
	}
}

// TestScheduleFor_SpecChangeReinstalls 钉住 upsert 的另一面：spec 变化 ⇒
// 摘旧装新——旧表达式立即停止触发，新表达式从注册时刻起算。
func TestScheduleFor_SpecChangeReinstalls(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 16)
	if err := s.ScheduleFor("k", "*/5 * * * * *", collectFires(mark, "old")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(5 * time.Second)
	if got := waitMark(t, mark, 2*time.Second); got != "old@5" {
		t.Fatalf("首次触发标记 = %q，期望 old@5", got)
	}

	// 改表达式：重装
	if err := s.ScheduleFor("k", "*/10 * * * * *", collectFires(mark, "new")); err != nil {
		t.Fatal(err)
	}

	// T+10：旧 spec 本会触发（old 的下一个点是 T+10），重装后应只有 new 触发
	clock.Advance(5 * time.Second)
	if got := waitMark(t, mark, 2*time.Second); got != "new@10" {
		t.Fatalf("重装后触发标记 = %q，期望 new@10（旧 spec 已摘除）", got)
	}
	// 通道里不得再有 old 的触发
	select {
	case m := <-mark:
		t.Fatalf("旧 spec 重装后仍触发：%q", m)
	default:
	}
}

// TestScheduleFor_Errors 钉住防呆面：空 key / nil task / 坏 spec / 不可满足
// spec / 已停止，均报错且不影响既有注册。
func TestScheduleFor_Errors(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 8)
	if err := s.ScheduleFor("k", "@every 10s", collectFires(mark, "keep")); err != nil {
		t.Fatal(err)
	}

	if err := s.ScheduleFor("", "*/5 * * * * *", func(context.Context, time.Time) error { return nil }); err == nil {
		t.Error("空 key 应报错")
	}
	if err := s.ScheduleFor("k2", "*/5 * * * * *", nil); err == nil {
		t.Error("nil task 应报错")
	}
	if err := s.ScheduleFor("k2", "not a cron", func(context.Context, time.Time) error { return nil }); err == nil {
		t.Error("坏 spec 应报错")
	}
	if err := s.ScheduleFor("k2", "0 0 31 2 *", func(context.Context, time.Time) error { return nil }); !errors.Is(err, ErrUnsatisfiableSpec) {
		t.Errorf("不可满足 spec 应返回 ErrUnsatisfiableSpec，got %v", err)
	}
	// 失败的 upsert 不影响既有注册：k 仍按原 spec 触发
	start(t, s)
	clock.Advance(10 * time.Second)
	if got := waitMark(t, mark, 2*time.Second); got != "keep@10" {
		t.Fatalf("失败 upsert 影响了既有注册：标记 = %q，期望 keep@10", got)
	}

	// 已停止的调度器拒绝新注册
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Stop(ctx)
	if err := s.ScheduleFor("k3", "*/5 * * * * *", func(context.Context, time.Time) error { return nil }); err == nil {
		t.Error("停止后 ScheduleFor 应报错")
	}
}

// TestUnscheduleFor 钉住按业务键注销：移除后停止触发，未知 key 返回 false。
func TestUnscheduleFor(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 8)
	if err := s.ScheduleFor("k", "*/5 * * * * *", collectFires(mark, "k")); err != nil {
		t.Fatal(err)
	}
	if !s.UnscheduleFor("k") {
		t.Error("UnscheduleFor 应返回 true")
	}
	if s.UnscheduleFor("k") {
		t.Error("重复 UnscheduleFor 应返回 false")
	}
	if s.UnscheduleFor("unknown") {
		t.Error("未知 key 应返回 false")
	}
	start(t, s)

	clock.Advance(10 * time.Second)
	select {
	case m := <-mark:
		t.Errorf("注销后仍触发了：%s", m)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestScheduleFor_MultipleKeysSameSpec 钉住：同一 spec 可挂多个业务键，
// 各自独立触发（调度平台两个任务共用同一表达式是合法形态）。
func TestScheduleFor_MultipleKeysSameSpec(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 16)
	if err := s.ScheduleFor("k1", "@every 5s", collectFires(mark, "k1")); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleFor("k2", "@every 5s", collectFires(mark, "k2")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(5 * time.Second)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := waitMark(t, mark, 2*time.Second)
		seen[m[:2]] = true
	}
	if !seen["k1"] || !seen["k2"] {
		t.Errorf("同 spec 多 key 应各自触发，seen=%v", seen)
	}
}

// TestScheduleFor_CoexistsWithSpecKeyForm 钉住两种形态并存不互通：同一 spec
// 经 Schedule（spec 键）与 ScheduleFor（业务键）各注册一次会各自触发。
func TestScheduleFor_CoexistsWithSpecKeyForm(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 16)
	if err := s.Schedule("@every 5s", collectFires(mark, "spec")); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleFor("k", "@every 5s", collectFires(mark, "key")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(5 * time.Second)
	seen := waitMarkSet(t, mark, 2, 2*time.Second)
	if !seen["spec"] || !seen["key"] {
		t.Errorf("spec 键与业务键形态应并存各自触发，seen=%v", seen)
	}
}

// TestReconcile_Converges 钉住对账语义（#129）：期望集收敛——消失项注销、
// 新项安装、未变项幂等（不重置 @every 触发基准）、重复对账空操作。
func TestReconcile_Converges(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 32)
	// A：@every 5s（首触发 T+5）；B：*/7（T+7、T+14…）
	if err := s.ScheduleFor("a", "@every 5s", collectFires(mark, "a")); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleFor("b", "*/7 * * * * *", collectFires(mark, "b")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	clock.Advance(7 * time.Second) // T+7：a@5、b@7（到达顺序不保证）
	seen := waitMarkSet(t, mark, 2, 2*time.Second)
	if !seen["a"] || !seen["b"] {
		t.Fatalf("首阶段触发标记集合 = %v，期望 {a b}", seen)
	}

	// 对账：期望集 = {b(未变), c(新增)}——a 消失 ⇒ 注销；b 未变 ⇒ 幂等
	//（@every/固定周期均不重置）；c 安装（@every 4s，自 T+7 起算 ⇒ T+11）
	if err := s.Reconcile([]Entry{
		{Key: "b", Spec: "*/7 * * * * *", Task: collectFires(mark, "b")},
		{Key: "c", Spec: "@every 4s", Task: collectFires(mark, "c")},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	clock.Advance(4 * time.Second) // T+11：c@11；a 已注销（本会 a@10）；b 未到点（T+14）
	if got := waitMark(t, mark, 2*time.Second); got != "c@11" {
		t.Fatalf("标记 = %q，期望 c@11", got)
	}
	select {
	case m := <-mark:
		t.Fatalf("期望集外的 a 未注销或未变的 b 被重装：%q（期望只有 c@11）", m)
	default:
	}

	// 重复对账同一期望集：幂等空操作
	if err := s.Reconcile([]Entry{
		{Key: "b", Spec: "*/7 * * * * *", Task: collectFires(mark, "b")},
		{Key: "c", Spec: "@every 4s", Task: collectFires(mark, "c")},
	}); err != nil {
		t.Fatalf("重复 Reconcile: %v", err)
	}
	clock.Advance(3 * time.Second) // T+14：b@14（未重置）；c 的下一点 T+15 未到
	if got := waitMark(t, mark, 2*time.Second); got != "b@14" {
		t.Fatalf("标记 = %q，期望 b@14（重复对账不得重装 b）", got)
	}
	select {
	case m := <-mark:
		t.Fatalf("重复对账触发了多余任务：%q", m)
	default:
	}
}

// TestReconcile_BadSpecDoesNotBlock 钉住对账容错：单条 spec 解析失败 /
// 不可满足不阻断其余条目的收敛，错误逐条汇总返回。
func TestReconcile_BadSpecDoesNotBlock(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 8)
	if err := s.ScheduleFor("doomed", "*/5 * * * * *", collectFires(mark, "doomed")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	err := s.Reconcile([]Entry{
		{Key: "doomed", Spec: "0 0 31 2 *", Task: collectFires(mark, "doomed")}, // 不可满足 ⇒ 该条失败
		{Key: "good", Spec: "@every 5s", Task: collectFires(mark, "good")},      // 正常安装
	})
	if err == nil {
		t.Fatal("存在失败条目应返回错误")
	}
	if !errors.Is(err, ErrUnsatisfiableSpec) {
		t.Errorf("错误应包含 ErrUnsatisfiableSpec，got %v", err)
	}

	// good 仍收敛成功并触发；doomed 保持旧 spec（重装失败 ⇒ 旧条目不动）
	clock.Advance(5 * time.Second) // T+5：doomed@5（旧 spec）、good@5
	seen := waitMarkSet(t, mark, 2, 2*time.Second)
	if !seen["doomed"] || !seen["good"] {
		t.Errorf("坏 spec 条目应保旧不动、其余条目应正常收敛，seen=%v", seen)
	}
}

// mIdx 取标记里 '@' 前的 key 部分。
func mIdx(m string) int {
	for i := 0; i < len(m); i++ {
		if m[i] == '@' {
			return i
		}
	}
	return len(m)
}

// markKey 取标记的 key 部分（'@' 前）。
func markKey(m string) string { return m[:mIdx(m)] }

// waitMarkSet 等待 n 个标记并返回 key 集合（触发顺序不保证——dispatchDue
// 按 map 遍历派发，多任务同时到期时到达顺序随机）。
func waitMarkSet(t *testing.T, mark <-chan string, n int, timeout time.Duration) map[string]bool {
	t.Helper()
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		select {
		case m := <-mark:
			seen[markKey(m)] = true
		case <-time.After(timeout):
			t.Fatalf("等待第 %d 个标记超时，已见 %v", i+1, seen)
		}
	}
	return seen
}

// TestReconcile_FailClosedOnDuplicateKeys 钉住 fail-closed：期望集内 key
// 重复 ⇒ 歧义，本次调用不做任何变更。
func TestReconcile_FailClosedOnDuplicateKeys(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})

	mark := make(chan string, 8)
	if err := s.ScheduleFor("a", "@every 5s", collectFires(mark, "a")); err != nil {
		t.Fatal(err)
	}
	start(t, s)

	err := s.Reconcile([]Entry{
		{Key: "a", Spec: "*/7 * * * * *", Task: collectFires(mark, "a")},
		{Key: "a", Spec: "@every 5s", Task: collectFires(mark, "a")},
	})
	if err == nil {
		t.Fatal("期望集内 key 重复应报错")
	}
	// 未做任何变更：a 仍按旧 spec 触发（T+5）
	clock.Advance(5 * time.Second)
	if got := waitMark(t, mark, 2*time.Second); got != "a@5" {
		t.Fatalf("fail-closed 未生效，a 的注册被改动：标记 = %q，期望 a@5", got)
	}
}

// TestReconcile_NilTaskForNewKey 钉住：新增条目缺 Task ⇒ 该条失败（不装
// 半个没有任务函数的条目），错误汇总返回。
func TestReconcile_NilTaskForNewKey(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})
	start(t, s)

	if err := s.Reconcile([]Entry{{Key: "ghost", Spec: "*/5 * * * * *"}}); err == nil {
		t.Fatal("新增条目缺 Task 应报错")
	}
	// 未安装：ghost 不触发
	clock.Advance(10 * time.Second)
	if n := s.pendingCount(); n != 0 {
		t.Errorf("ghost 不应被安装，pending=%d", n)
	}
	if s.UnscheduleFor("ghost") {
		t.Error("ghost 不应被安装")
	}
}

// TestReconcile_Stopped 钉住：已停止的调度器拒绝对账。
func TestReconcile_Stopped(t *testing.T) {
	s, _ := newTestScheduler(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Stop(ctx)
	if err := s.Reconcile([]Entry{{Key: "k", Spec: "*/5 * * * * *", Task: func(context.Context, time.Time) error { return nil }}}); err == nil {
		t.Error("停止后 Reconcile 应报错")
	}
}

// TestKeyed_ConcurrentMutation_NoRace 并发形态冒烟：ScheduleFor /
// UnscheduleFor / Reconcile 与调度循环并发跑，-race 下无数据竞争、无 panic。
func TestKeyed_ConcurrentMutation_NoRace(t *testing.T) {
	s, clock := newTestScheduler(t, Config{})
	start(t, s)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load() && i < 200; i++ {
			_ = s.ScheduleFor(fmt.Sprintf("k%d", i%8), "@every 1s", func(context.Context, time.Time) error { return nil })
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load() && i < 200; i++ {
			s.UnscheduleFor(fmt.Sprintf("k%d", i%8))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load() && i < 50; i++ {
			_ = s.Reconcile([]Entry{
				{Key: "rc", Spec: "@every 2s", Task: func(context.Context, time.Time) error { return nil }},
			})
		}
	}()

	// 并发推进时钟驱动调度循环
	for i := 0; i < 20; i++ {
		clock.Advance(500 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
}
