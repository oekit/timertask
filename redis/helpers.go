package redis

import (
	"context"
	"fmt"

	"github.com/oekit/timertask"
)

// runHandlerSafe 就地收编 handler panic 为任务失败：async.GoSafe 的 recover 在
// goroutine 出口，panic 会直接打死 worker，N 个 panic 任务耗尽整个 worker 池后
// 队列塞满、消费停滞，整个后端瘫痪。收编后 panic 任务走既有重试/死信路径。
//
// 与根包 github.com/oekit/timertask 的同名内部函数同源：Redis 后端是独立
// module，无法访问根包的未导出符号，故在此保留一份。
func runHandlerSafe(ctx context.Context, handler timertask.Handler, task *timertask.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(ctx, task)
}

// normalizeRetry 把非法的负 MaxRetry 归一为 0（至少执行一次、不重试），
// 避免"未执行即进死信"的假死信。
//
// 与根包 github.com/oekit/timertask 的同名内部函数同源（独立 module 无法访问
// 根包未导出符号）。
func normalizeRetry(task *timertask.Task) {
	if task.MaxRetry < 0 {
		task.MaxRetry = 0
	}
}
