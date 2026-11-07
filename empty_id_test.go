package timertask

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLocalBackend_Send_EmptyID 回归测试：空任务 ID 必须被拒绝。
//
// 缺陷成因：ID 注释写着「任务唯一 ID」，但两个后端都不校验 ⇒ 空 ID 的任务
// 在 local 后端只有第一个入队、其余静默丢弃，在 redis 后端则全部入队、各自
// 执行一次。构造期直接报错，把「调用方漏填 ID」挡在提交那一刻。
func TestLocalBackend_Send_EmptyID(t *testing.T) {
	b := NewLocalBackend(func(ctx context.Context, task *Task) error { return nil })

	err := b.Send(context.Background(), &Task{EndTime: time.Now().Unix()})
	if !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("空 ID 应返回 ErrEmptyTaskID，实得 %v", err)
	}
}

// TestLocalBackend_Send_DuplicateID 钉住同 ID 重复提交的口径：先到先得，
// 第二次不入队、返回 nil（幂等提交成功）。
//
// #97 后 Send 不再就地改写调用方 Task：重复 Send（被去重挡下，本来就没入队）
// 与首次 Send 一样不补齐调用方对象的 CreateTime——缺省补齐只落在引擎内部
// 副本上（本断言此前钉的是 #97 移除的改写行为）。
func TestLocalBackend_Send_DuplicateID(t *testing.T) {
	b := NewLocalBackend(func(ctx context.Context, task *Task) error { return nil })

	ctx := context.Background()
	first := &Task{ID: "dup-1", EndTime: time.Now().Unix()}
	if err := b.Send(ctx, first); err != nil {
		t.Fatalf("首次 Send: %v", err)
	}
	if first.CreateTime != 0 {
		t.Errorf("首次 Send 不应改写调用方 CreateTime（#97），实得 %d", first.CreateTime)
	}
	second := &Task{ID: "dup-1", EndTime: time.Now().Unix()}
	if err := b.Send(ctx, second); err != nil {
		t.Fatalf("重复 Send 应返回 nil（幂等提交成功），实得 %v", err)
	}
	if second.CreateTime != 0 {
		t.Errorf("重复 Send 不应改写调用方 CreateTime（#97），实得 %d", second.CreateTime)
	}
}
