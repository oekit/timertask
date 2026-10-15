# timertask

> 定时任务微库——分片定时任务（FNV-1a hash、心跳续约）

> 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit) 拆分矩阵的成员（Round-4 微库化）。

## 安装

```bash
go get github.com/oekit/timertask@v0.1.3
```

## 快速使用

```go
import "github.com/oekit/timertask"

var handler timertask.Handler = func(ctx context.Context, t *timertask.Task) error {
    return dispatch(t.Action, t.Param)
}
backend := timertask.NewLocalBackend(handler)
_ = backend.Start(ctx)
_ = backend.Send(ctx, &timertask.Task{ID: "t1", EndTime: time.Now().Add(time.Minute).Unix(), Action: "remind"})
```

## 包清单

- 根包 `timertask` — 基于 [ekit-concurrency](https://github.com/oekit/concurrency)、[ekit-logx](https://github.com/oekit/logx)

依赖：[ekit-concurrency](https://github.com/oekit/concurrency)（async）、[ekit-logx](https://github.com/oekit/logx)。

## License

见 [LICENSE](LICENSE)
