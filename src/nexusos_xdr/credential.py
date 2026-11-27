#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""credential.py — Task392 P0: 凭据完整性体系签发/校验/防伪模块。

凭据 = 行为合法发生的必要条件证明。
Task532 P-d3 文档对齐现实：当前仅实施 dispatch_cred 单向（done_cred 无签发点，
宣称与实现不符——未来加签发点时同步更新此处）。

签发：服务端在 dispatch() 路由内签发（Agent 无法自造）
校验：服务端在 /callback 路由内校验（无凭据 = fail-open warn）
防伪：HMAC-SHA256，secret 在 $NEXUS_HOME/secrets/cred-secret（0600）
"""
import hashlib
import hmac
import json
import logging
import os
import secrets as _secrets
import re

# 2611230963 戊：凭据号形状（以 issue_credential 的签发格式为准：cred_ + 16 位小写 hex）
CRED_ID_RE = re.compile(r"cred_[0-9a-f]{16}")


def mask_credential_ref(text: str) -> str:
    """把文本里的凭据号打码成引用（前 8 位 + 省略号），非凭据内容不动。

    用途：台账/回调正文等「留存但不使用」的面——不再落明文全号。
    派单凭据 credential_id 是回调校验要读的，不走这里。
    """
    return CRED_ID_RE.sub(lambda m: m.group(0)[:8] + "…", str(text))


def _mask_obj(obj):
    """递归打码 dict/list/str 结构里的凭据号（append_callback 用）。"""
    if isinstance(obj, str):
        return mask_credential_ref(obj)
    if isinstance(obj, dict):
        return {k: _mask_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask_obj(v) for v in obj]
    return obj


import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行4）

import psycopg2

logger = logging.getLogger(__name__)

CRED_DB = "nexus_mem"
CRED_TTL_HOURS = 24
SECRET_PATH = Path(os.environ.get("NEXUS_XDR_CRED_FILE") or subpath("secrets/cred-secret"))


def _get_secret() -> str:
    """读或生成 HMAC secret（首次自动生成，0600 权限）。"""
    if SECRET_PATH.exists():
        return SECRET_PATH.read_text(encoding="utf-8").strip()
    # 首次生成
    _secret = _secrets.token_hex(32)
    SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
    SECRET_PATH.write_text(_secret, encoding="utf-8")
    os.chmod(SECRET_PATH, 0o600)
    logger.info(f"[credential] HMAC secret 首次生成: {SECRET_PATH} (0600)")
    return _secret


def _get_conn():
    """获取 PG 连接（复用 nexus_mem 库）。"""
    return psycopg2.connect(f"dbname={CRED_DB}")


def _compute_signature(payload: dict) -> str:
    """HMAC-SHA256 签名（对 sort_keys 后的 JSON 签名，防篡改）。
    
    payload 只含稳定字段（type/bearer/lease_token/prerequisites），
    不含时间（时间精度在 PG 存取时可能变，单独做过期校验）。
    """
    secret = _get_secret()
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hmac.new(
        secret.encode("utf-8"), payload_json.encode("utf-8"), hashlib.sha256
    ).hexdigest()[:32]


def issue_credential(
    cred_type: str,
    bearer: str,
    prerequisites: Optional[dict] = None,
    ttl_hours: int = CRED_TTL_HOURS,
    task_id: Optional[str] = None,  # Task563: 绑定任务（凭据反查 task_id 归因 apply_attempt 用）
) -> Optional[str]:
    """签发凭据 → 写 PG credentials 表 → 返回 credential_id。

    Args:
        cred_type: dispatch_cred / done_cred
        bearer: 持有方 agent 名
        prerequisites: 前置流程完成证明（如 gate_passed/references_md_exists）
        ttl_hours: 过期时间（默认 24h）
        task_id: 关联任务 ID（Task563）。dispatch 签发时传——消费端
            （api /search）凭据验证后反查此列，为 apply_attempt 补 task_id
            归因（修 /tmp 上下文文件不可用时的断裂，如主控/任务间隙场景）

    Returns:
        credential_id（UUID）或 None（签发失败）
    """
    # 2611230901：检索凭据的签发收口——机器名单内的名字不签发（登记与派单
    # 两条路径都走这里；判断下沉到本函数=唯一咽喉点）。导入失败照常签发但留痕。
    if cred_type == "search_cred":
        _bearer_norm = bearer.strip().lower()
        try:
            from nexusos_mem.apply_honesty import MACHINE_AGENTS as _MA_901
            if _bearer_norm in {n.strip().lower() for n in _MA_901}:
                logger.warning(f"[2611230901] 机器名单内身份 {bearer} 不签发 search_cred（防绕过不记套用）")
                return None
        except Exception as _e:
            # 2611239006（红队五轮 G2）：名单查不了就不发——原来「照常签发并留痕」是出错放行，
            # 机器身份能借此拿到检索凭据（绕过「不记套用」）。默认值翻到拒绝一侧。
            logger.warning(f"[2611230901] 机器名单导入失败，身份 {bearer} 无法判定，**拒绝签发**"
                           f"（2611239006 翻到拒绝侧；原因: {type(_e).__name__}: {_e}）")
            return None

    try:
        conn = _get_conn()
        cur = conn.cursor()

        # SEQUENCE 取 lease_token（单调递增）
        cur.execute("SELECT nextval('credentials_lease_seq')")
        lease_token = cur.fetchone()[0]

        credential_id = f"cred_{uuid.uuid4().hex[:16]}"
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=ttl_hours)
        prerequisites = prerequisites or {}

        # 签名 payload（只含稳定字段，不含时间——时间精度在 PG 存取时可能变）
        sig_payload = {
            "type": cred_type,
            "bearer": bearer,
            "lease_token": lease_token,
            "prerequisites": prerequisites,
        }
        signature = _compute_signature(sig_payload)

        cur.execute(
            """
            INSERT INTO credentials
                (credential_id, cred_type, bearer, issued_at, expires_at,
                 prerequisites, lease_token, signature, status, task_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'ISSUED', %s)
            RETURNING credential_id
            """,
            (
                credential_id,
                cred_type,
                bearer,
                now,
                expires_at,
                json.dumps(prerequisites, ensure_ascii=False),
                lease_token,
                signature,
                task_id,  # Task563: 任务绑定凭据（None 保持旧行为）
            ),
        )
        result_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"[credential] 签发 {cred_type} for {bearer}: "
            f"id={result_id} lease={lease_token} expires={expires_at.isoformat()}"
        )
        return result_id
    except Exception as e:
        logger.warning(f"[credential] 签发失败 ({cred_type}/{bearer}): {e}")
        try:
            conn.rollback()
            cur.close()
            conn.close()
        except Exception:
            pass
        return None


def verify_credential(
    credential_id: str,
    expected_type: str,
    expected_bearer: str = "",
) -> tuple[bool, str]:
    """校验凭据 → 验签 + 类型 + 过期 + status。

    Args:
        credential_id: 凭据 ID
        expected_type: 期望类型（dispatch_cred / done_cred）
        expected_bearer: 期望持有者 Agent 名（Task532 P-d6: 可选——传入时校验
            bearer 一致性，防 A 的凭据被 B 冒用；空串跳过该层向后兼容）

    Returns:
        (valid, detail): valid=True 时 detail="凭据有效"，False 时 detail=失败原因
    """
    if not credential_id:
        return False, "无 credential_id"

    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cred_type, bearer, issued_at, expires_at,
                   prerequisites, lease_token, signature, status
              FROM credentials
             WHERE credential_id = %s
            """,
            (credential_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row is None:
            return False, f"凭据不存在: {credential_id}"

        cred_type, bearer, issued_at, expires_at, prerequisites, lease_token, signature, status = row

        # Task532 P-d6: bearer 一致性校验（传入 expected_bearer 时生效——
        # 防 A 的凭据被 B 冒用；空串跳过向后兼容）
        if expected_bearer and bearer and bearer != expected_bearer:
            return False, f"凭据持有者不匹配: 凭据 bearer={bearer}, 请求方={expected_bearer}"

        # 1. 验签（只验稳定字段，时间单独做过期校验）
        sig_payload = {
            "type": cred_type,
            "bearer": bearer,
            "lease_token": lease_token,
            "prerequisites": prerequisites if isinstance(prerequisites, dict) else json.loads(prerequisites or "{}"),
        }
        expected_sig = _compute_signature(sig_payload)
        if not hmac.compare_digest(signature, expected_sig):
            return False, "签名不匹配（凭据伪造或篡改）"

        # 2. 类型匹配
        if cred_type != expected_type:
            return False, f"类型不匹配: 期望 {expected_type}, 得到 {cred_type}"

        # 3. 未过期
        now = datetime.now(timezone.utc)
        if hasattr(expires_at, "tzinfo") and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if now > expires_at:
            return False, f"凭据已过期 (expires_at={expires_at.isoformat()})"

        # 4. status 检查
        if status not in ("ISSUED", "ACTIVE"):
            return False, f"凭据状态异常: {status}"

        return True, f"凭据有效 (bearer={bearer}, lease={lease_token})"

    except Exception as e:
        logger.warning(f"[credential] 校验异常 ({credential_id}): {e}")
        return False, f"校验异常: {type(e).__name__}: {e}"


def archive_expired_credentials() -> dict:
    """Task395 P3: 过期凭据归档（ISSUED→EXPIRED→ARCHIVED 两阶段）。

    1. ISSUED 且过期 → 标 EXPIRED
    2. EXPIRED 且超 1 天 → 标 ARCHIVED（不删数据, D15 废弃不删除）

    Returns: {"expired": N, "archived": N}
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE credentials SET status='EXPIRED' "
            "WHERE status='ISSUED' AND expires_at < NOW()"
        )
        expired = cur.rowcount
        cur.execute(
            "UPDATE credentials SET status='ARCHIVED' "
            "WHERE status='EXPIRED' AND expires_at < NOW() - INTERVAL '1 day'"
        )
        archived = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if expired or archived:
            logger.info(f"[credential] 归档: {expired} 条→EXPIRED, {archived} 条→ARCHIVED")
        return {"expired": expired, "archived": archived}
    except Exception as e:
        logger.warning(f"[credential] 归档失败: {e}")
        return {"expired": 0, "archived": 0, "error": str(e)}


def verify_proof(text: str, project_root: str = "") -> tuple[bool, str, str]:
    """Task394 P2: 校验文本中的证据指针（硬凭据+软凭据）。

    硬凭据（服务端机械验证）：
      proof:file:<path>:<L42-L58>   → 读文件确认行号存在
      proof:test:<cmd>              → 格式校验（P2不实际跑）
      proof:grep:<pattern>:<path>   → 格式校验（P2不实际跑）
      proof:psql:<query>            → 格式校验（P2不实际跑）

    软凭据（只校验结构非空，内容靠 Layer2 抽查）：
      proof:reasoning:<text>        → 非空校验(≥10字符)
      proof:analogy:<ref>           → 非空校验
      proof:user_endorsement        → 100%可信（一级信号）
      proof:manual:<text>           → 主观声明豁免

    Returns:
        (verified, proof_type, detail)
        - proof_type: "hard" / "soft" / "none"
        - verified: True=至少1个有效凭据
    """
    import re as _re
    if not text:
        return False, "none", "无文本"

    # 提取所有 proof:type:value 模式（value 可选, 如 proof:user_endorsement 无值也匹配）
    _PROOF_RE = _re.compile(r"proof:(\w+)(?::([^\n]+))?", _re.IGNORECASE)
    matches = _PROOF_RE.findall(text)
    if not matches:
        return False, "none", "无 proof 证据指针"

    _HARD_TYPES = {"file", "test", "grep", "psql"}
    _SOFT_TYPES = {"reasoning", "analogy", "user_endorsement", "manual"}

    has_hard = False
    has_soft = False
    details = []

    for ptype, pval in matches:
        ptype = ptype.lower().strip()
        pval = pval.strip()

        if ptype in _HARD_TYPES:
            has_hard = True
            if ptype == "file" and pval:
                ok, msg = _verify_file_proof(pval, project_root)
                if not ok:
                    return False, "hard", f"硬凭据校验失败: proof:file:{pval} — {msg}"
            details.append(f"hard:{ptype}")
        elif ptype in _SOFT_TYPES:
            has_soft = True
            if ptype == "reasoning" and len(pval) < 10:
                return False, "soft", "软凭据校验失败: proof:reasoning 内容过短(<10字符)"
            details.append(f"soft:{ptype}")

    if has_hard:
        return True, "hard", f"含硬凭据({', '.join(details[:3])})"
    if has_soft:
        return True, "soft", f"含软凭据({', '.join(details[:3])}), soft_verified=false"
    return False, "none", "无有效凭据类型"


def _verify_file_proof(spec: str, project_root: str = "") -> tuple[bool, str]:
    """校验 proof:file:path:L42-L58 指向的文件+行号存在。"""
    from pathlib import Path
    parts = spec.rsplit(":", 1)  # 分离 path 和行号
    if len(parts) != 2:
        # 没行号也接受（只校验文件存在）
        fpath = Path(spec)
        line_range = None
    else:
        fpath_str, line_range = parts
        fpath = Path(fpath_str)

    if not fpath.is_absolute() and project_root:
        fpath = Path(project_root) / fpath

    if not fpath.exists():
        return False, f"文件不存在: {fpath}"
    if line_range:
        try:
            # 解析 L42-L58 或 L42
            line_range = line_range.lstrip("Ll")
            if "-" in line_range:
                start, end = line_range.split("-", 1)
                start_line = int(start)
            else:
                start_line = int(line_range)
            total_lines = sum(1 for _ in open(fpath, encoding="utf-8", errors="replace"))
            if start_line > total_lines:
                return False, f"行号 {start_line} 超出文件总行数 {total_lines}"
        except (ValueError, OSError) as e:
            return False, f"行号解析失败: {e}"
    return True, "文件+行号有效"


def _self_test():
    """模块自测——验证签发/校验/过期/防伪。"""
    print("=== credential.py self_test ===")

    # 1. 签发
    cred_id = issue_credential(
        cred_type="dispatch_cred",
        bearer="test-agent",
        prerequisites={"gate_passed": True, "task_spec_complete": True},
    )
    assert cred_id, "签发失败"
    print(f"  ✅ 签发成功: {cred_id}")

    # 2. 正常校验
    valid, detail = verify_credential(cred_id, "dispatch_cred")
    assert valid, f"校验失败: {detail}"
    print(f"  ✅ 校验通过: {detail}")

    # 3. 类型不匹配
    valid, detail = verify_credential(cred_id, "done_cred")
    assert not valid, "类型不匹配应失败"
    print(f"  ✅ 类型不匹配拦截: {detail}")

    # 4. 不存在的凭据
    valid, detail = verify_credential("cred_nonexistent", "dispatch_cred")
    assert not valid, "不存在凭据应失败"
    print(f"  ✅ 不存在凭据拦截: {detail}")

    # 5. 无 credential_id
    valid, detail = verify_credential("", "dispatch_cred")
    assert not valid, "空 ID 应失败"
    print(f"  ✅ 空 ID 拦截: {detail}")

    # 6. HMAC secret 存在
    assert SECRET_PATH.exists(), "secret 文件应存在"
    mode = oct(SECRET_PATH.stat().st_mode & 0o777)
    print(f"  ✅ HMAC secret: {SECRET_PATH} (权限 {mode})")

    print("=== self_test 全通过 ===")


if __name__ == "__main__":
    _self_test()
