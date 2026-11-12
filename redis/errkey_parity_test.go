package redis

import (
	"testing"

	"github.com/oekit/logx"
	"github.com/oekit/timertask"
)

// #193 对照钉子：根模块 ErrKey（slog-only 依赖政策下的仓内单源）与
// logx.ErrKey 必须同值——错误字段跨 local（slog 落盘）与 redis（logx 落
// Loki/ES）两条形态必须收敛到同一个 key。漂移即本测试红。
func TestErrKeyParityWithLogx(t *testing.T) {
	if timertask.ErrKey != logx.ErrKey {
		t.Fatalf("timertask.ErrKey = %q, logx.ErrKey = %q——错误字段键漂移（#193 红线）",
			timertask.ErrKey, logx.ErrKey)
	}
}
