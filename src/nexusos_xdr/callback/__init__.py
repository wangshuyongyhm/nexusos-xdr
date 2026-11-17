"""nexusos_xdr.callback — done 回调域子包（Task618 #57 主线批）。

域职责：小弟 done/failed/progress 回调的 HTTP 入口 + bg-gate 后台检查 +
清算钩子（auto-relocate/experiences 写 MEM/apply_candidates/Phase2 WARN）。

架构契约（T604 三要素）：register_routes(mcp) 显式注册；本包禁 import
mcp_server（依赖方向 mcp_server → callback 单向）；mcp_server 顶部 shim
re-export http_callback（外部引用/测试 patch 语义不变）。

Task492 线程语义保持：bg-gate daemon 线程在 done 热路径后台跑（不阻塞
callback 响应）——搬运零字符变化。
"""
from nexusos_xdr.callback.handler import (
    http_callback,
    register_routes,
)

__all__ = ["http_callback", "register_routes"]
