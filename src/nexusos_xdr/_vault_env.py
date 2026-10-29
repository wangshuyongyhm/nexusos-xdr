"""nexusos_xdr._vault_env — vault 级依赖的包内自足替代（Task668 批4b ②；1957 锚点收敛）。

替代两类 vault 级 import（650 独立发现的外泄面，9 文件顶层 import
`nexus.constants` / `nexus.paths_setup`——该包不在切片集，干净环境
pip install 后 import 即炸）：

1. `from nexus.constants import VAULT_ROOT`
   → 本模块 VAULT_ROOT：锚点解析单源 nexusos_common（1957；env 优先 > 哨兵推导）。
2. `import nexus.paths_setup`（副作用：_AUTO_DIR/_XDR_DIR/_SCRIPTS_DIR 入 sys.path）
   → setup_sys_path()：vault 布局存在时做同款副作用（哨兵=common 单源）；
   不存在（wheel 独立部署/干净环境）跳过。

缺 vault 行为（1955 design §3.2 统一：可降级但禁静默）：xdr 状态层须
vault-less 可跑——降级哨兵路径 + 显式 WARN（存在性检查自然失败，不再无
声无息）；dispatch 等必须 vault 的功能由各自入口显式报错。降级哨兵随
任务1960（~/.hermes 家族迁移）一并消除。
"""
import logging
import os
import sys
from pathlib import Path

from nexusos_common import (
    VAULT_MISSING_REMEDIATION,
    auto_scripts_dir,
    subpath,
    vault_root,
)

logger = logging.getLogger(__name__)

_root = vault_root(__file__)
if _root is not None:
    VAULT_ROOT = _root
else:
    logger.warning(
        "NEXUS vault 不可用——vault 相对路径解析降级为不存在路径（状态层保持可跑，"
        "vault 相对目标的存在性检查自然失败）；dispatch 等必须 vault 的功能请先修复。"
        + VAULT_MISSING_REMEDIATION
    )
    VAULT_ROOT = Path("/nonexistent-nexus-vault-root")  # 降级哨兵（WARN 后非静默；单2 消除）


def setup_sys_path() -> None:
    """vault 布局的 sys.path 初始化（nexus.paths_setup 的包内等价；哨兵=common 单源）。

    仅当包位于 vault 内（自动化脚本/ 且含 nexus/）时生效——复用
    nexus.paths_setup 单源避免逻辑 drift；wheel 独立部署/干净环境（无 vault
    nexus/）no-op。
    """
    auto_dir = auto_scripts_dir(__file__)
    if auto_dir is None:
        return  # 非 vault 布局（批4 wheel 部署）：无旧位可指，pip 依赖承接
    if str(auto_dir) not in sys.path:
        sys.path.insert(0, str(auto_dir))
    try:
        import nexus.paths_setup  # noqa: F401  副作用单源：尽力而为（原 Task668 语义保持）
    except Exception:  # noqa: BLE001, S110
        pass


def resolve_gate_vault_root(context: dict = None):  # noqa: RUF013 —— T678 原签名保持（调用方兼容）
    """T678 调用方契约：gate YAML `{vault_root}` 模板的注入源 + 缺 key 防线。

    解析序（PARAMETERIZATION-GUIDE 既有规范）：context 显式值 > env
    NEXUS_VAULT_ROOT > 本模块 VAULT_ROOT 缺省（nexusos-common 单源，1957）。
    防线（任务书 §5.3）：解析结果非存在目录（含未替换的 `{...}` 字面量）时
    返回 (False, 人话错误)——调用方显式报错，绝不带着字面量进 gate
    （_resolve_template 对缺 key 静默返回原串，cwd/vault_root 会变非法
    路径——668 §二实证的陷阱1）。

    Returns:
        (True, vault_root_str)：解析成功；context 非 None 时 setdefault 注入
        (False, error_str)：显式失败（调用方 exit/返回错误，fail-fast）
    """
    ctx = context if context is not None else {}
    vr = str(ctx.get("vault_root") or os.environ.get("NEXUS_VAULT_ROOT") or str(VAULT_ROOT))
    if "{" in vr or not Path(vr).is_dir():
        return False, (
            f"vault_root 不可用（解析值 {vr!r} 非存在目录）。gate YAML 已全量 "
            f"{{vault_root}} 模板化（T678），缺该 key 会静默落字面量致 cwd/路径非法"
            f"——显式拒绝。设 NEXUS_VAULT_ROOT 或在 context 传 vault_root"
        )
    if context is not None:
        ctx.setdefault("vault_root", vr)
    return True, vr


# agents/base.py 的 task_monitor 增强路径需要 scripts 目录可达。
# 该增强自带 try/except 静默降级（不可用→关键词主路径），此处仅提供
# 可选副作用：目录存在才加（干净环境 no-op，行为不变）。
# （1960：默认值迁 .nexus/xdr/scripts，design §2.1 行7；公开 SCRIPTS_DIR 供 dispatchd 复用）
SCRIPTS_DIR = Path(os.environ.get("NEXUS_XDR_SCRIPTS_DIR") or subpath("xdr/scripts"))
if SCRIPTS_DIR.is_dir() and str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
