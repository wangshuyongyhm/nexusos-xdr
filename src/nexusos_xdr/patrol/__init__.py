"""nexusos_xdr.patrol — patrol 监控域子包（Task609 #57 批次：最难批）。

域职责：dispatch 后的 B 兜底监控（四重确认判 done/stuck 检测/超时宽限/
step-cost cap/webhook 通知/屏障 checkpoint 清算）。

架构契约（T604 试点三要素复用 + 本批新增注入模式）：
    - 依赖方向：dispatchd → patrol 单向；本包禁 import dispatchd。
    - runtime 注入：dispatchd 域有状态成员（PipePaneContext/
      _capture_apply_candidates/_consume_barrier_checkpoint/_active_patrols/
      trigger_auto_relocate/apply_capture_enabled）经 bind_deps(dispatchd_module)
      绑定——dispatchd import 本包后立即调用（见 dispatchd.py 模块尾）。
    - 状态收敛：十个单元素 list 闭包惯用法 → PatrolState dataclass（state.py）。
    - shim：dispatchd.py re-export 本包公开名（外部引用/测试 patch 不变——
      同一函数对象）。

Task609 迁入清单（自 dispatchd.py :546-1184，行为零变化）：
    _check_output_completion_triple / _scan_task_dir_latest_mtime
    / _timeout_grace_decision / _start_patrol_thread
    + 纯函数随迁：parse_cost_from_pane（含 cost 正则常量）/ _post_webhook_with_retry
    / _find_task_root（patrol 私有副本——dispatchd 原定义保留供其他域用）
"""
from nexusos_xdr.patrol.state import PatrolState
from nexusos_xdr.patrol.loop import (
    _check_output_completion_triple,
    _scan_task_dir_latest_mtime,
    _timeout_grace_decision,
    _start_patrol_thread,
    parse_cost_from_pane,
    _post_webhook_with_retry,
    bind_deps,
    check_pending_silence_once,
    ensure_pending_silence_patrol,
)

__all__ = [
    "PatrolState",
    "_check_output_completion_triple",
    "_scan_task_dir_latest_mtime",
    "_timeout_grace_decision",
    "_start_patrol_thread",
    "parse_cost_from_pane",
    "_post_webhook_with_retry",
    "bind_deps",
    "check_pending_silence_once",
    "ensure_pending_silence_patrol",
]
