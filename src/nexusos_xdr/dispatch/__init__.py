"""nexusos_xdr.dispatch — dispatch 主流程域子包（Task621 #57 收官批）。

域职责：任务派发主函数（屏障/凭据/副本/wait 模式）+ 查询/取消/审批/Agent 清单。

架构契约：bind_deps(dispatchd_module) 注入（禁 import dispatchd 防循环——
锁/错误/预检/清理/wait/_capture/常量在 dispatchd 域）；patrol 正向 import；
dispatchd.py shim re-export（外部 `dispatchd.dispatch` 调用方零变化）。
"""
from nexusos_xdr.dispatch.main import (
    dispatch,
    check_status,
    cancel_task,
    request_approval,
    list_supported_agents,
    _loop_goal_context,
    _write_dispatch_barrier_checkpoint,
    _consume_barrier_checkpoint,
    bind_deps,
)

__all__ = [
    "dispatch",
    "check_status",
    "cancel_task",
    "request_approval",
    "list_supported_agents",
    "bind_deps",
    "_loop_goal_context",
    "_write_dispatch_barrier_checkpoint",
    "_consume_barrier_checkpoint",
]
