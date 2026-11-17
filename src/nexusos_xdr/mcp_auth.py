#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.mcp_auth — MCP 层凭据中间件（Task 1417 L0+L1 实施）。

FastMCP AuthMiddleware 的 auth check 实现。分层语义（1411 report 安全三层设计，
本批实施 L0+L1，L2 留二批）：

    L0 匿名只读：search_knowledge / presence_active / presence_events /
        check_task_status —— anonymous 放行（对齐 HTTP 侧 anonymous 豁免语义，
        B12 人类兜底通道同款设计）。
    L1 凭据写：write_knowledge —— 必须 Authorization: Bearer <search_cred>，
        verify_credential 校验；无效凭据 fail-closed 拒写（绝不降级 anonymous，B12）。
    L2 高危调度（1420 收官批开放）：dispatch / cancel_task —— 必须
        Authorization: Bearer <l2_dispatch_cred>（独立凭据类型，不复用
        search_cred——裸 search_cred 拿不到调度权；发放见指针目录
        ~/.config/nexus/l2-credentials/ 600）。
    其余（request_approval/视觉三件套/两自检/presence 写类/gate_check/
        apply_experience_outcome）：仍不开放，auth check 恒 False。

实现位置（探针实证，task/1417 process/full-log §阶段2）：AuthMiddleware 的
auth check 里 get_http_request() 能读到 Authorization header（streamable-http
每请求透传），不需要 HTTP 中间件层改造；mcp SDK 1.28.1 客户端无 headers 参数，
带凭据须自构造 httpx.AsyncClient 透传（探针实证的唯一正解）。
"""
import logging

from fastmcp.server.auth import AuthContext

logger = logging.getLogger("nexusos-xdr.mcp")

# 分层工具集（1411 report L0/L1 名单；L2 与内部工具不在此列=不可见）
L0_TOOLS = {
    "search_knowledge",
    "presence_active",
    "presence_events",
    "check_task_status",
}
L1_TOOLS = {
    "write_knowledge",
}
L2_TOOLS = {
    "dispatch",
    "cancel_task",
}

# 凭据类型：L1 写复用 search_cred（MEM 检索凭据同款——同一 Agent 的知识库
# 读写凭据，credentials 表 cred_type 枚举实测：dispatch_cred/search_cred/
# orchestrator_search_cred）
L1_CRED_TYPE = "search_cred"
L2_CRED_TYPE = "l2_dispatch_cred"  # 1420 新增：调度权与读写权隔离


def _verify_cred_sync(auth_header: str, cred_type: str) -> bool:
    """同步校验凭据（L1/L2 共用）：Authorization → credential_id → verify_credential。

    无凭据/格式错/校验失败一律 False（fail-closed，B12 语义照抄 HTTP 侧）。
    """
    cred = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else ""
    if not cred:
        return False
    try:
        from nexusos_xdr.credential import verify_credential
        valid, detail = verify_credential(cred, cred_type, "")
        if not valid:
            logger.warning("mcp_auth %s 凭据无效: %s", cred_type, detail)
        return valid
    except Exception:  # 校验链异常=不可信，拒（不因异常放行）
        logger.exception("mcp_auth %s 凭据校验异常", cred_type)
        return False


async def nexus_mcp_auth(ctx: AuthContext) -> bool:
    """AuthMiddleware 的 auth check：按工具名分层判定。

    L0 恒放行；L1 校验 Authorization 凭据；其余（L2/内部）恒拒——
    list_tools 过滤可见性 + call_tool 拦截，两层都走这里。
    """
    tool_name = ctx.component.name if ctx.component else ""
    if tool_name in L0_TOOLS:
        return True
    if tool_name in L1_TOOLS:
        try:
            from fastmcp.server.dependencies import get_http_request
            req = get_http_request()
            auth_header = req.headers.get("authorization", "")
        except Exception:
            return False  # 拿不到请求上下文=fail-closed
        # verify_credential 是同步 psycopg2 调用——转线程跑，不阻塞事件循环
        import anyio
        return await anyio.to_thread.run_sync(_verify_cred_sync, auth_header, L1_CRED_TYPE)
    if tool_name in L2_TOOLS:
        # L2 调度权：独立 l2_dispatch_cred 类型校验——分层边界：
        # search_cred（只读写）不能派单（1420 契约表）。
        try:
            from fastmcp.server.dependencies import get_http_request
            req = get_http_request()
            auth_header = req.headers.get("authorization", "")
        except Exception:
            return False
        import anyio
        return await anyio.to_thread.run_sync(_verify_cred_sync, auth_header, L2_CRED_TYPE)
    return False  # 其余工具：本批不开放
