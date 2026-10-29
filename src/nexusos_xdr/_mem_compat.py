#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr._mem_compat — nexusos-mem 伴随包的容错接入（Task1597 炸点①）。

nexusos-mem 不发 PyPI（三件套同仓安装），nexusos-xdr 已把对它的依赖降为
[mem] extra——干净环境单装 xdr 时它可能缺席。这里把「顶层 import 不炸 +
用到时报人话错误」收敛到一个真源：缺包时 env 净化等接入点抛出带安装
指引的 RuntimeError，而不是让用户对着 ModuleNotFoundError 一脸懵。
"""
try:
    # 正源在伴随包（pyproject [mem] extra；Task668 ② 包路径口径）
    from nexusos_mem.env_scrub import scrubbed_env
except ImportError:
    scrubbed_env = None

MEM_MISSING_MSG = (
    "nexusos-mem 未安装（nexusos-xdr 的伴随包，非 PyPI 分发）："
    "pip install ./pypi/nexusos-mem 或 pip install 'nexusos-xdr[mem]'；"
    "三件套安装顺序见 README 安装节。"
)
_MISSING_MSG = MEM_MISSING_MSG  # 兼容别名（本模块内部用）


def scrubbed_env_or_raise() -> dict:
    """env 净化入口：缺伴随包时抛带安装指引的错（fail-loud 但说人话）。"""
    if scrubbed_env is None:
        raise RuntimeError(_MISSING_MSG)
    return scrubbed_env()
