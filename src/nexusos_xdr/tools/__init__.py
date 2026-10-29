"""nexusos_xdr.tools — MCP 工具域子包（Task620 #57）。

域职责：18 个 @mcp.tool 工具（dispatch/知识库/task 查询/视觉预留/
presence MCP 面/gate_check）+ 域内辅助（_forward_to_ocrx/_issue_search_cred）。

架构契约（T604 三要素）：register_tools(mcp, get_kb) 显式注册（get_kb
注入不反向 import）；mcp_server 顶部 shim re-export 全部工具名（外部
引用/测试 patch 语义不变——同一函数对象）。
"""
from nexusos_xdr.tools.registry import (
    dispatch,
    search_knowledge,
    apply_experience_outcome,
    write_knowledge,
    request_approval,
    check_task_status,
    cancel_task,
    capture_screen,
    _forward_to_ocrx,
    detect_popup_visual,
    analyze_ui_state,
    verify_mcp_config,
    check_last_mile,
    _issue_search_cred,
    presence_register,
    presence_active,
    presence_heartbeat,
    presence_events,
    presence_declare_op,
    gate_check,
    register_tools,
)

__all__ = [
    "register_tools",
    "dispatch",
    "search_knowledge",
    "apply_experience_outcome",
    "write_knowledge",
    "request_approval",
    "check_task_status",
    "cancel_task",
    "capture_screen",
    "detect_popup_visual",
    "analyze_ui_state",
    "verify_mcp_config",
    "check_last_mile",
    "presence_register",
    "presence_active",
    "presence_heartbeat",
    "presence_events",
    "presence_declare_op",
    "gate_check",
]
