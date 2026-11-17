"""nexusos_xdr.httpapi — HTTP 任务 API 域子包（Task621 #57 收官批）。

域职责：/dispatch /task/{id} /task_state /cancel/{id} 四端点（kimi curl fallback 面）。

架构契约：register_routes(mcp) 显式注册；mcp_server shim re-export
http_dispatch/http_task_status/http_task_state_list/http_cancel（外部引用不变）。
"""
from nexusos_xdr.httpapi.routes import (
    http_dispatch,
    http_task_status,
    http_task_state_list,
    http_cancel,
    register_routes,
)

__all__ = [
    "http_dispatch",
    "http_task_status",
    "http_task_state_list",
    "http_cancel",
    "register_routes",
]
