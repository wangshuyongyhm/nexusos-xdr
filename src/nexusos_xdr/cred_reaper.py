#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cred_reaper.py — 凭据 reaper 定时清理入口（Task441 backlog 4 彻底修复）。

MEM#38698 调研结论：credential.py 的 archive_expired_credentials() 已实现正确，
但定时调度从未接，导致过期凭据滞留 status=ISSUED。本 wrapper 只做"接通调度"——
不改动 credential.py 任何逻辑。

行为：
  1. 调 archive_expired_credentials()（幂等 UPDATE，绝不 DELETE）
     - ISSUED 且过期 → EXPIRED
     - EXPIRED 且超 1 天 → ARCHIVED
  2. 打印扫描/变更结果到 stdout（launchd → nexus-cred-reaper.out.log）
  3. exit 0 成功 / 非 0 失败（launchd 据此判定）

PG 启动窗重试（Task1559 ②，对照 scheduler._ensure_table 30s×3 先例）：开机/重启日
launchd RunAtLoad 先于 PG 就绪 → 连接类错误退避重试后再判失败（1548 F4：历史同型
8 次丢一天）。env：NEXUS_REAPER_PG_RETRIES（默认 3）/ NEXUS_REAPER_PG_RETRY_DELAY
（默认 30 秒；测试注入小值）。

launchd：etc/plists/ai.nexus.cred-reaper.plist（StartInterval 86400 + RunAtLoad）

用法：
  python3 cred_reaper.py            # launchd 调
  python3 cred_reaper.py --dry-run  # 只报告不执行（仅打印）
"""
import logging
import os
import sys
import time
from pathlib import Path

# sys.path 初始化（独立脚本风格，参照 crucible_trigger.py）
# 1636 活①评估: 切片入 src 后 _AUTO_DIR 语义=src/(包根)而非 vault 自动化脚本/,
# 名称沿用但作用是脚本自举(裸跑 cred_reaper.py 时可解析 nexusos_xdr 包)——无害保留
_HERE = Path(__file__).resolve().parent
_AUTO_DIR = _HERE.parent
for _p in (str(_AUTO_DIR), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# nexus.constants.VAULT_ROOT（不硬编码 vault 路径）
try:
    from nexusos_xdr._vault_env import VAULT_ROOT  # Task668: 包内自足（干净环境可解析）
except Exception:  # 兜底：constants 不可用时只警告（reaper 不依赖 vault 路径运行）
    VAULT_ROOT = Path(os.environ.get("NEXUS_VAULT_ROOT") or "/nonexistent-nexus-vault-root")

# INFO→stdout（out.log），WARN+→stderr（error.log），参照 crucible 日志分流
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [cred-reaper] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("cred-reaper")
_err_handler = logging.StreamHandler(sys.stderr)
_err_handler.setLevel(logging.WARNING)
_err_handler.setFormatter(logging.Formatter("%(asctime)s [cred-reaper] %(levelname)s: %(message)s"))
logger.addHandler(_err_handler)

from nexusos_xdr.credential import archive_expired_credentials  # noqa: E402  Task631/633 迁包: 裸名→包路径

# 连接类错误特征（libpq 报文子串）——只退避这类瞬态错误，其余错误立即判失败
_RETRYABLE_MARKERS = ("connection", "running locally", "reset by peer")


def _is_retryable_error(err: str) -> bool:
    return any(m in err for m in _RETRYABLE_MARKERS)


def main(dry_run: bool = False) -> int:
    logger.info(f"VAULT_ROOT={VAULT_ROOT}  dry_run={dry_run}")
    if dry_run:
        logger.info("--dry-run：仅打印意图，不调用 archive_expired_credentials()")
        logger.info("dry-run 完成（无 DB 改动）")
        return 0

    retries = max(0, int(os.environ.get("NEXUS_REAPER_PG_RETRIES", "3")))
    retry_delay = float(os.environ.get("NEXUS_REAPER_PG_RETRY_DELAY", "30"))
    for attempt in range(retries + 1):
        try:
            result = archive_expired_credentials()
        except Exception as e:
            logger.error(f"调用 archive_expired_credentials 失败: {type(e).__name__}: {e}")
            return 2

        err = result.get("error")
        if not err:
            break
        if _is_retryable_error(err) and attempt < retries:
            logger.warning(
                f"PG 未就绪，{retry_delay:g}s 后重试 {attempt + 1}/{retries}: "
                f"{err.splitlines()[0]}")
            time.sleep(retry_delay)  # c2-allow: PG 启动窗口就绪重试（Task1559/1563 实证竞态族），对齐 scheduler.py 重试先例
            continue
        logger.error(f"archive_expired_credentials 报错: {err}")
        return 3

    expired = result.get("expired", 0)
    archived = result.get("archived", 0)

    logger.info(f"扫描完成: {expired} 条 ISSUED→EXPIRED, {archived} 条 EXPIRED→ARCHIVED")
    # reaper 是幂等的，0 改动也属正常成功（无过期凭据时）
    return 0


if __name__ == "__main__":
    _dry = "--dry-run" in sys.argv
    sys.exit(main(dry_run=_dry))
