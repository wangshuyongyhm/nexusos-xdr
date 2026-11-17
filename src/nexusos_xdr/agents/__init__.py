#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.agents — OSS 公开层（私 fleet 不随包，1976 拍板）。

注册表=$NEXUS_HOME/xdr/agents.d/*.yaml + 内置 demo（见 public.py docstring）。
"""
from nexusos_xdr.agents.public import (  # noqa: F401
    AGENT_REGISTRY, ConfigAgent, get_agent, get_agent_status,
    list_agents, list_agents_with_status, load_registry,
)
