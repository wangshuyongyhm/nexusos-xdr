#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nexusos-xdr — 所有 Agent 的通信协议层

与 nexusos-mem（记忆层）并列，负责 Agent 之间的:
- 任务派发（dispatch）
- 状态同步（task lifecycle）
- 审批代理（approval routing）
- 过程落盘（auto-archive）

入口: python3 -m nexusos_xdr.mcp_server（或 mcp_server.py 直接运行）
端口: 19998（MCP /mcp + HTTP /dispatch /task/<id>）

作者: ZCode Agent  日期: 2026-07-13
Task631/633 迁包: 正源自 vault 内 nexusos_xdr/ 迁入 pip 包（旧位 __init__ 以
__path__ 重定向到本包——旧位 shim/配置文件作为子模块回退源继续可用）。
"""

__version__ = "1.0.0"

# ── 路径自愈（批3 过渡层，批2 nexusos_mem 同款）────────────────────
# 迁移件仍引用 vault 级平铺件与 mem 域包（nexus.paths_setup 副作用、
# nexus.constants、nexus.shared、nexus_mem_utils、reembed_nulls、
# gate_engine 旧位 shim 等）。editable 安装时本包位于 vault 内
# pypi/nexusos-xdr/src/nexusos_xdr/，向上 4 层即 自动化脚本/，先入 sys.path
# 再复用 nexus.paths_setup 单源（_AUTO_DIR+_XDR_DIR+SCRIPTS_DIR[.nexus/xdr/scripts,1960]）。
# 批4 wheel 独立部署时本目录无 nexus/ → 跳过（由 pip 依赖承接）。
import sys as _sys
from pathlib import Path as _Path

_AUTO_DIR = _Path(__file__).resolve().parents[4]
_OLD_PKG_DIR = _AUTO_DIR / "nexusos_xdr"
if (_AUTO_DIR / "nexus").is_dir():
    # Task668 ②: 包内自足版（_vault_env.setup_sys_path 内部同款逻辑——vault
    # 布局才副作用，干净环境 no-op；替代对 vault 级 nexus.paths_setup 的顶层依赖）
    from nexusos_xdr import _vault_env as _ve_init  # noqa: E402
    _ve_init.setup_sys_path()
if _OLD_PKG_DIR.is_dir():
    # 旧位目录作为子模块回退源（etc/gates symlink、etc/hooks、agents 配置域
    # 脚本、gate_engine/env_scrub shim 只存在于旧位）——pip 解析上下文也可达
    __path__.append(str(_OLD_PKG_DIR))


def gate_check(scenario: str, context: dict, agent: str = "") -> dict:
    """过场景门禁（便捷导出，Task572 未决#2）。

    与 dispatchd.py 的 `from nexusos_gate.gate_engine import check` 完全等价，
    让用户/Agent 直觉写法 `from nexusos_xdr import gate_check` 可用。
    返回 {passed, failures, hard_fails, warns}。
    """
    from nexusos_gate.gate_engine import check
    return check(scenario, context, agent=agent)
