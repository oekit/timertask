module github.com/oekit/timertask/redis

go 1.26.0

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/oekit/concurrency v0.17.0
	github.com/oekit/logx v0.7.0
	github.com/oekit/timertask v0.2.3
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

// 本地开发用：父模块尚未发版时也能编译/测试。
// replace 只在「主模块」生效，下游消费本模块时被忽略（按 require 的版本解析）。
replace github.com/oekit/timertask => ../
