"""nexusos_xdr._vault_deps — vault 级 nexus.tools_meta/checkpoint/auto_approval 的
包内安全解析（Task674 ②，照 _vault_env.py 模式）。

问题（668 发现）：dispatchd 的 Task295 预检（nexus.tools_meta/auto_approval）
与 mcp_server 的 state_transition checkpoint 钩子（nexus.checkpoint）是顶层
`from nexus.xxx import ...`——依赖 sys.path 里有 `自动化脚本/`。flip 到 venv 后
包从 site-packages 加载，vault sys.path 不在 → import 全失败 → 钩子静默关闭
（恢复锚功能受损）。

解法（与 _vault_env 同模式）：
  1. 先直接 import（vault 内运行时 sys.path 本就有——零变化）；
  2. 失败 → 按 NEXUS_VAULT_ROOT（env 优先，缺省同 _vault_env）定位 vault 的
     `自动化脚本/` 塞 sys.path 再试一次（同机 flip 场景：vault 在机，功能保留）；
  3. 仍失败（真干净环境，无 vault）→ WARN + 返 None，调用方维持原有优雅降级
     语义（Task295 预检关/checkpoint 钩子降级——干净环境本无 tools.yaml/LOOP
     目录，降级是正确行为而非 bug）。1957：降级从静默改为 WARN（design §3.2
     可降级但禁静默）。
"""
import importlib
import logging
import sys
from pathlib import Path

from nexusos_common import VAULT_MISSING_REMEDIATION

logger = logging.getLogger(__name__)

_VAULT_ROOT = Path(__import__("nexusos_xdr._vault_env", fromlist=["VAULT_ROOT"]).VAULT_ROOT)


def _vault_nexus(module: str):
    """安全 import vault 级 nexus.<module>：vault 在→模块；不在→WARN+None。"""
    try:
        return importlib.import_module(module)
    except Exception:
        pass
    auto_dir = _VAULT_ROOT / "05-System" / "自动化脚本"
    if not (auto_dir / "nexus").is_dir():
        logger.warning(
            f"vault 级 {module} 不可用（干净环境降级 None：预检/checkpoint 降级）。"
            + VAULT_MISSING_REMEDIATION
        )
        return None  # 真干净环境（无 vault）：降级（1957 起带 WARN）
    if str(auto_dir) not in sys.path:
        sys.path.insert(0, str(auto_dir))
    try:
        return importlib.import_module(module)
    except Exception:
        return None


def _pick(module: str, names):
    """从 vault 级模块挑符号；模块不可用则全 None。"""
    mod = _vault_nexus(module)
    if mod is None:
        return (None,) * len(names)
    try:
        return tuple(getattr(mod, n) for n in names)
    except AttributeError:
        return (None,) * len(names)


# --- Task295：dispatchd 预检 API ---
should_auto_approve, precheck_for_agent = _pick("nexus.tools_meta",
                                                ("should_auto_approve", "precheck_for_agent"))
(log_tool_call,) = _pick("nexus.auto_approval", ("log_tool_call",))
TOOLS_META_READY = should_auto_approve is not None and precheck_for_agent is not None

# --- state_transition checkpoint 钩子 API ---
write_checkpoint, recover = _pick("nexus.checkpoint", ("write_checkpoint", "recover"))
CHECKPOINT_READY = write_checkpoint is not None and recover is not None
