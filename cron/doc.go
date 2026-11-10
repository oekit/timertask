// Package cron 提供多实例动态 cron 装配面（#159）：运行期增删改 cron 任务
// （指纹 diff 增量重装，不重置未变化项触发点）+ 多实例执行互斥（租约型，
// distlock 可选注入）+ 运行台账（执行历史/结果可观测）。
//
// 触发引擎复用同模块 local.Scheduler（表达式解析/catch-up/并发度/假时钟
// 都是既有基建），本包在其上补三块缺口：DB 计划的期望集收敛、跨实例
// 触发互斥、运行台账——此前下游 3 处各自手搓调度器，锁 TTL 预算公式这类
// 易错口径散落各处（见 RetryBudget）。
//
// 装配示例（调度平台形态，多实例共用 DB 期望集）：
//
//	sched := cron.New(exec,
//	    cron.WithMutex(distAdapter{locker}),        // redisx/distlock 等，见 Mutex
//	    cron.WithInstance("task-1"),                // 台账/互斥归因
//	)
//	// 库替你管循环：启动清扫 → 首轮对账 → 按周期收敛，ctx 取消停机
//	if err := sched.StartReconcile(ctx, 30*time.Second, listFromDB); err != nil {
//	    return err
//	}
//	defer sched.Close(context.Background())
//
// 也可自带循环周期调 Reconcile（同样自动拉起触发引擎）：
//
//	sched.Reconcile(ctx, []cron.Plan{{ID: "t1", Spec: "@every 1m", Enabled: true}})
//
// # 互斥语义（触发点恰好一次）
//
// 互斥键 = `timertask/cron/exec/<planID>/<trigger>`（trigger 为触发点
// RFC3339 标识，秒精度）——同一触发点跨实例同键裁决，只跑一次；让位方
// 记台账（Skipped）。持有即租约（TTL = RetryBudget.LockTTL()，到期自动
// 接管，崩溃实例不尸占），认领租约不随执行完成释放——兼作「slot 已认领」
// 标记，迟到重复触发在租约有效期内一律让位；互斥设施故障 fail-closed 跳过
// （宁可漏跑一轮可补发，不可双跑）。
//
// # 与 local 包的关系
//
// 本包是 github.com/oekit/timertask 的同模块子包，import local 复用触发
// 引擎；需要裸 cron 触发（无计划表/无互斥/无台账）直接用 local.Scheduler。
package cron
