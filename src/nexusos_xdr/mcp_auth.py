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
import time

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

# ============================================================================
# 2611192325：/mcp 端点验签（ASGI 层）
# ============================================================================
# 背景：1417 的 AuthMiddleware 只做「工具级」分层（L0 匿名只读 / L1 写凭据 /
# L2 调度权），**握手层不校验** —— 实测无 Authorization 的 POST /mcp 也 200，
# 而且 TUI 配置里的 token 早在 2026-09-19 就过期了却一直"能用"，坐实了端点不验签。
#
# 本类补的是「HTTP 握手层」：`/mcp` 前缀的请求必须带有效 Bearer 凭据，否则 401。
#   - 只拦 /mcp（/health、/session/*、/task/* 等 custom_route 一律放行——它们
#     不是 MCP 协议面，且被 monitor/其他 Agent 无凭据调用）
#   - 凭据接受 search_cred 或 l2_dispatch_cred（L1/L2 同源凭据体系，不新造 token）
#   - 无 header / 格式错 / 过期 / 撤销 → 401（fail-closed，B12 语义）
#   - 结果缓存：通过 60s、拒绝 10s —— MCP 每次请求都会带 header，不缓存等于
#     每请求打一次 PG；拒绝侧短 TTL 保证「刚续期的凭据」10s 内可用。
# 与工具级 AuthMiddleware 是**叠加**关系：这里过门，门内仍按 L0/L1/L2 分层。
class MCPHeaderAuthGate:
    """ASGI middleware：/mcp 握手层强制 Bearer 凭据。"""

    MCP_PREFIX = "/mcp"
    OK_TTL = 60.0
    FAIL_TTL = 10.0
    ACCEPTED_CRED_TYPES = ("search_cred", "l2_dispatch_cred")

    def __init__(self, app, ok_ttl: float = 60.0, fail_ttl: float = 10.0):
        self.app = app
        self.OK_TTL = ok_ttl
        self.FAIL_TTL = fail_ttl
        self._cache: dict[str, tuple] = {}

    # --- 内部 ---
    def _is_mcp_path(self, path: str) -> bool:
        return path == self.MCP_PREFIX or path.startswith(self.MCP_PREFIX + "/")

    @staticmethod
    def _extract_bearer(scope) -> str:
        for k, v in scope.get("headers") or []:
            if k.lower() == b"authorization":
                raw = v.decode("latin-1").strip()
                if raw.lower().startswith("bearer "):
                    return raw[7:].strip()
                return raw
        return ""

    def _verify(self, token: str) -> tuple:
        """返回 (ok, reason)。带短 TTL 缓存。"""
        now = time.time()
        hit = self._cache.get(token)
        if hit and hit[2] > now:
            return hit[0], hit[1]
        ok, reason = False, "无凭据"
        if token:
            try:
                from nexusos_xdr.credential import verify_credential
            except Exception as e:  # 凭据子系统不可用 → fail-closed 但留痕
                logger.error(f"[MCPGate] credential 模块不可用（fail-closed）: {e}")
                ok, reason = False, "凭据校验子系统不可用"
            else:
                reasons = []
                for ct in self.ACCEPTED_CRED_TYPES:
                    try:
                        valid, detail = verify_credential(token, ct)
                    except Exception as e:
                        valid, detail = False, f"校验异常 {type(e).__name__}: {e}"
                    if valid:
                        ok, reason = True, f"{ct} 有效"
                        break
                    reasons.append(f"{ct}: {detail}")
                if not ok:
                    reason = "；".join(reasons)[:200]
        self._cache[token] = (ok, reason, now + (self.OK_TTL if ok else self.FAIL_TTL))
        return ok, reason

    @staticmethod
    async def _reject(send, reason: str) -> None:
        import json as _json
        body = _json.dumps({
            "error": "unauthorized",
            "detail": f"/mcp 需 Authorization: Bearer <search_cred|l2_dispatch_cred>（{reason}）",
            "hint": "凭据三来源：①xdr register 响应的 search_credential_id ②dispatch 副本 frontmatter "
                    "③静态文件 05-System/Agent/<name>/search_cred.txt",
        }, ensure_ascii=False).encode()
        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [(b"content-type", b"application/json; charset=utf-8"),
                        (b"content-length", str(len(body)).encode()),
                        (b"www-authenticate", b'Bearer realm="nexus-xdr"')],
        })
        await send({"type": "http.response.body", "body": body})

    # --- ASGI 入口 ---
    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or not self._is_mcp_path(scope.get("path", "")):
            await self.app(scope, receive, send)
            return
        token = self._extract_bearer(scope)
        ok, reason = self._verify(token)
        if ok:
            await self.app(scope, receive, send)
            return
        logger.warning(f"[MCPGate] 401 {scope.get('method')} {scope.get('path')} from "
                       f"{scope.get('client')} — {reason[:120]}")
        await self._reject(send, reason)
