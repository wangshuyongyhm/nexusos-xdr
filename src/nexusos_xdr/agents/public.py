#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agents/public.py — OSS 公开配置驱动 Agent 层（Task 1976，O8 收口）。

vault 私 fleet（oc/omp/kimi/ds/... 诸 Agent 实现）不随包分发——OSS 用户经
`$NEXUS_HOME/xdr/agents.d/*.yaml` 注册自己的 tmux 目标：

    - name: myagent          # dispatch `to` 用此名
      tmux_session: work     # 目标 tmux session（须已运行且空闲）
      status: active         # active/paused（paused 不被 dispatch）

内置 `demo` 条目（tmux_session=demo）开箱即有 dispatch 目标——`tmux new-session
-d -s demo` 后即可 hello-task。投递语义=AgentBase 通用流（is_idle→send_keys
任务文本→提交键→验证）。vault 生产侧不 import 本模块（私 fleet 不受影响）。
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

from nexusos_xdr.agents.base import AgentBase

logger = logging.getLogger("nexusos-xdr.agents.public")

_BUILTIN = {"demo": {"tmux_session": "demo", "status": "active"}}


class ConfigAgent(AgentBase):
    """配置驱动 Agent（name/tmux_session 来自注册表条目）。"""

    def __init__(self, name: str, tmux_session: str):
        self.name = name
        self.tmux_session = tmux_session


def _registry_dir() -> Path:
    import nexusos_common
    return nexusos_common.subpath("xdr", "agents.d")


def load_registry() -> Dict[str, dict]:
    """内置 demo + 用户 $NEXUS_HOME/xdr/agents.d/*.yaml（后者覆盖同名内置）。"""
    registry = dict(_BUILTIN)
    d = _registry_dir()
    if not d.is_dir():
        return registry
    import yaml
    for f in sorted(d.glob("*.yaml")):
        try:
            entries = yaml.safe_load(f.read_text(encoding="utf-8")) or []
        except Exception as e:  # noqa: BLE001 —— 单文件损坏跳过（注册表 fail-soft）
            logger.warning(f"agents.d/{f.name} 解析失败跳过: {e}")
            continue
        if isinstance(entries, dict):
            entries = [entries]
        for entry in entries:
            if isinstance(entry, dict) and entry.get("name"):
                registry[str(entry["name"])] = {
                    "tmux_session": str(entry.get("tmux_session") or entry["name"]),
                    "status": str(entry.get("status") or "active"),
                }
    return registry


AGENT_REGISTRY: Dict[str, dict] = load_registry()


def get_agent(name: str) -> Optional[AgentBase]:
    """获取 Agent 实例（非 active 跳过——与 vault 侧 get_agent 语义一致）。"""
    entry = AGENT_REGISTRY.get(name)
    if entry is None or entry.get("status") != "active":
        return None
    return ConfigAgent(name, entry["tmux_session"])


def get_agent_status(name: str) -> Optional[str]:
    entry = AGENT_REGISTRY.get(name)
    return entry.get("status") if entry else None


def list_agents(active_only: bool = True) -> List[str]:
    if active_only:
        return sorted(n for n, e in AGENT_REGISTRY.items() if e.get("status") == "active")
    return sorted(AGENT_REGISTRY)


def list_agents_with_status() -> Dict[str, str]:
    return {n: e.get("status", "unknown") for n, e in AGENT_REGISTRY.items()}
