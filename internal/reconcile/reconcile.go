// Package reconcile 收敛 local/cron 两层 Reconcile 的共享骨架（#261）。
//
// 两层对账（local.Reconcile 按业务键、cron.Reconcile 按计划 ID）对期望集
// 做**逐行对应**的 fail-closed 校验（空 ID / 重复 ID ⇒「以哪条为准」存在
// 歧义，整体拒绝零变更），此前两份实现各自维护、漂移风险随单源消除——
// 对账语义的变更（如放宽/收紧校验口径）自此只改一处。
package reconcile

import (
	"errors"

	pkgerrors "github.com/oekit/pkg/errors"
)

// ValidateDesiredSet 对账期望集的 fail-closed 校验（#261 单源）：
//   - 空 ID：无法标识条目 ⇒ 歧义，返回错误；
//   - 重复 ID：同一 ID 出现多条，「以哪条为准」存在歧义 ⇒ 返回错误。
//
// 调用方收到错误必须**整体拒绝本次对账**（零变更），不得半量收敛——
// fail-closed 的语义由调用方保证，本函数只负责判定与报错。
//
// prefix 是错误文案的包前缀（"cron" / "local"），noun 是条目名词
// （"plan ID" / "key"）——错误文案与两层既有输出逐字节一致（含
// pkgerrors.Newf 的重复 ID 值），调用方测试无感。
func ValidateDesiredSet(prefix, noun string, ids []string) error {
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" {
			return errors.New(prefix + ": reconcile: empty " + noun + " in desired set")
		}
		if seen[id] {
			return pkgerrors.Newf("%s: reconcile: duplicate %s %q in desired set", prefix, noun, id)
		}
		seen[id] = true
	}
	return nil
}
