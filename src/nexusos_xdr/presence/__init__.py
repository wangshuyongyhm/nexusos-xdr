"""nexusos_xdr.presence — NEXUS-PRESENCE 在场感知域子包（Task604 #57 批次③试点）。

域职责：Agent 在场感知 HTTP 路由（register/unregister/heartbeat/active/
event/events/broadcast/op-log），状态层走 nexusos_xdr.session_state。

架构契约（试点模式）：
    - 显式注册：宿主（mcp_server）调 register_routes(mcp, ...) 挂路由；
      本包禁止 import mcp_server（依赖方向 mcp_server → presence 单向）。
    - shim：mcp_server 顶部 re-export 本包 9 函数（显式列名），保持任何
      `mcp_server.http_session_*` 外部引用/测试 patch 语义不变（同一函数对象）。

Task604 迁入清单（自 mcp_server.py :1575-1879，行为零变化）：
    http_session_register / unregister / heartbeat / active / event / events
    / broadcast / op_log / op_log_query
"""
from nexusos_xdr.presence.routes import (
    http_session_register,
    http_session_unregister,
    http_session_heartbeat,
    http_session_active,
    http_session_event,
    http_session_events,
    http_session_broadcast,
    http_session_op_log,
    http_session_op_log_query,
    register_routes,
)

__all__ = [
    "register_routes",
    "http_session_register",
    "http_session_unregister",
    "http_session_heartbeat",
    "http_session_active",
    "http_session_event",
    "http_session_events",
    "http_session_broadcast",
    "http_session_op_log",
    "http_session_op_log_query",
]
