#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""layer2_calibrate.py — Task406: Layer2 校准反馈层。

配对引擎（apply_attempt 预测 vs apply_outcome 真实）→ 偏差量化 → 校准因子 → 影响 search 排序。

冷启动三档（ds 第7轮设计）：
  <10 配对：不校准（calibration_weight 保持 1.0）
  10-50 配对：弱校准（±10% 微调）
  >50 配对：正常校准

不改 A-Z 字母值，只在排序层乘法叠加 calibration_weight。
"""
import json
import logging
from datetime import datetime, timezone

import psycopg2

logger = logging.getLogger(__name__)

DB = "nexus_mem"
CALIB_MIN = 0.3
CALIB_MAX = 2.0
# Wave3(2026-08-14): 10→5。Wave2 清洗+expire 后配对池增大，但单次 run 配对数
# 受 apply_outcome 时序影响。降到 5 让弱校准(multiplier=0.1)在数据稀疏期提前生效
COLD_START_NO_CALIB = 5
COLD_START_WEAK = 50       # 10-50 弱校准(±10%) / >50 正常


def _get_conn():
    return psycopg2.connect(f"dbname={DB}")


def pair_predictions_with_outcomes():
    """配对引擎：找 apply_attempt（预测）和对应的 apply_outcome（真实结果）。

    配对键：apply_outcome.detail->>'exp_id' = apply_attempt.target_id
    + 时间窗口（outcome 在 attempt 之后）

    Returns:
        [(exp_id, prediction_seq, prediction_uncertainty, outcome_result), ...]
    """
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                ao.target_id AS exp_id,
                aa.detail->>'seq' AS pred_seq,
                aa.detail->>'uncertainty' AS pred_uncertainty,
                ao.detail->>'result' AS outcome_result,
                ao.created AS outcome_time
            FROM agent_activity ao
            JOIN agent_activity aa
                ON aa.id::text = ao.detail->>'apply_attempt_id'
                AND aa.action = 'apply_attempt'
            WHERE ao.action = 'apply_outcome'
              AND ao.target_id IS NOT NULL
              -- Task516: 过滤测试污染——apply_attempt_id=0 是无效占位（真实回填必有有效 aa_id）
              AND COALESCE(ao.detail->>'apply_attempt_id', '0') != '0'
              -- Task516: 已知测试 agent 不参与校准（q2-verify 曾致 47 条全 pending 假配对）
              AND ao.agent_id NOT IN ('q2-verify', 'test-400', 'test-400-final')
              -- Task581: pending 是 Task412 通电占位非真实结果，滤出配对（与第二分支对齐，
              -- 08-19 审计实测 62 条 pending 假配对曾由此进入校准）
              -- Task984: not_applicable 同理滤出——评审裁定非真套用，不产出校准信号
              AND COALESCE(ao.detail->>'result', '') NOT IN ('pending', '', 'not_applicable')
            UNION ALL
            -- Task532 P-s3: inferred_apply 路径——expire 直接 UPDATE knowledge.outcome
            -- 不写 apply_outcome 行，原查询配对不到。从 knowledge 表补读（有 outcome
            -- 且无显式回填行的 exp，与 apply_attempt 按 exp 配对，走下方 per-exp 去重）
            SELECT
                k.id AS exp_id,
                aa.detail->>'seq' AS pred_seq,
                aa.detail->>'uncertainty' AS pred_uncertainty,
                k.outcome AS outcome_result,
                k.updated AS outcome_time
            FROM knowledge k
            JOIN agent_activity aa
                ON aa.target_id = k.id
                AND aa.action = 'apply_attempt'
            WHERE k.outcome IS NOT NULL
              AND k.outcome NOT IN ('pending', '')
              AND NOT EXISTS (
                  SELECT 1 FROM agent_activity ao2
                  WHERE ao2.action = 'apply_outcome'
                    AND ao2.target_id = k.id
                    AND COALESCE(ao2.detail->>'apply_attempt_id', '0') != '0'
                    AND ao2.agent_id NOT IN ('q2-verify', 'test-400', 'test-400-final')
              )
            ORDER BY exp_id, outcome_time DESC
        """)
        rows = cur.fetchall()
        # 去重：每个 exp_id 只取最近一对
        seen = set()
        pairs = []
        for exp_id, seq, unc, result, _time in rows:
            if exp_id in seen:
                continue
            seen.add(exp_id)
            pairs.append({
                "exp_id": exp_id,
                "pred_seq": seq or "",
                "pred_uncertainty": unc or "M",
                "outcome": result or "pending",
            })
        return pairs
    finally:
        cur.close()
        conn.close()


def quantify_deviation(pred_seq: str, pred_uncertainty: str, outcome: str) -> float:
    """偏差量化。

    偏差值：
      -1.0 = 假阳性（推荐套用但失败 outcome=failed + pred 推荐）
      -0.5 = 假阴性（没推荐但成功 outcome=success + pred 不推荐）
      +1.0 = 正确（outcome=success + pred 推荐）
      +0.5 = 正确偏保守（outcome=success + pred 条件推荐）
       0.0 = 不确定（outcome=pending/uncertain）

    pred 推荐 = uncertainty in (L, M)（低中不确定=倾向推荐）
    pred 不推荐 = uncertainty = H（高不确定=倾向不推荐）
    """
    is_recommended = pred_uncertainty in ("L", "M", None, "")
    outcome_success = outcome in ("success", "likely_success")
    outcome_failed = outcome in ("failed",)

    if outcome_failed and is_recommended:
        return -1.0  # 假阳性：推荐了但失败
    if outcome_success and not is_recommended:
        return -0.5  # 假阴性：没推荐但成功
    if outcome_success and is_recommended:
        if pred_uncertainty == "M":
            return 0.5  # 正确偏保守
        return 1.0  # 正确
    if outcome == "partial":
        return 0.3  # Task532 P-s2: partial=弱正信号（原落 0.0 是校准盲区——套用部分成功也是信号）
    return 0.0  # 不确定（pending/uncertain）


def update_calibration_weight(exp_id: int, deviation: float, weak_mode: bool = True):
    """更新校准因子。

    初始 1.0，偏差累加影响：
      - 弱校准模式（10-50 配对）：deviation × 0.1（±10% 微调）
      - 正常模式（>50 配对）：deviation × 0.3（±30% 调整）

    下限 0.3 上限 2.0。
    """
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT calibration_weight FROM knowledge WHERE id = %s", (exp_id,)
        )
        row = cur.fetchone()
        if not row:
            return None
        current = float(row[0] or 1.0)

        # 弱校准模式：偏差影响 ×0.1；正常模式 ×0.3
        multiplier = 0.1 if weak_mode else 0.3
        adjustment = deviation * multiplier
        new_weight = max(CALIB_MIN, min(CALIB_MAX, current + adjustment))

        if abs(new_weight - current) > 0.001:
            cur.execute(
                "UPDATE knowledge SET calibration_weight = %s, updated = NOW() WHERE id = %s",
                (new_weight, exp_id),
            )
            conn.commit()
            logger.info(f"Layer2: exp[{exp_id}] calibration_weight {current:.3f} → {new_weight:.3f} (deviation={deviation}, {'弱' if weak_mode else '正常'}模式)")
        return new_weight
    except Exception as e:
        logger.warning(f"Layer2: 更新 exp[{exp_id}] 校准因子失败: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def run() -> dict:
    """批量校准：跑配对→量化→更新。返回统计。"""
    pairs = pair_predictions_with_outcomes()
    pair_count = len(pairs)

    # 冷启动判断
    if pair_count < COLD_START_NO_CALIB:
        logger.info(f"Layer2: 配对数 {pair_count} < {COLD_START_NO_CALIB}, 冷启动不校准")
        return {"pairs": pair_count, "calibrated": 0, "mode": "cold_start_no_calib"}

    weak_mode = pair_count < COLD_START_WEAK
    mode = "weak" if weak_mode else "normal"
    logger.info(f"Layer2: 配对数 {pair_count}, {'弱' if weak_mode else '正常'}校准模式")

    calibrated = 0
    for p in pairs:
        deviation = quantify_deviation(p["pred_seq"], p["pred_uncertainty"], p["outcome"])
        if deviation != 0.0:
            result = update_calibration_weight(p["exp_id"], deviation, weak_mode=weak_mode)
            if result is not None:
                calibrated += 1

    return {"pairs": pair_count, "calibrated": calibrated, "mode": mode}


def flag_needs_review():
    """Task407 项1: 软凭据 Layer2 抽查——calibration_weight < 0.5 的经验标 needs-review。

    不自动删（D15），只加 tags 'needs-review' 供 search 返回时 warning。
    返回标记数。
    """
    conn = _get_conn()
    cur = conn.cursor()
    try:
        # 查 calibration_weight < 0.5 且未 deprecated 且未标 needs-review
        cur.execute("""
            SELECT id FROM knowledge
            WHERE calibration_weight < 0.5
              AND COALESCE(deprecated, FALSE) = FALSE
              AND superseded_by IS NULL
              AND NOT 'needs-review' = ANY(COALESCE(tags, ARRAY[]::text[]))
        """)
        ids = [row[0] for row in cur.fetchall()]
        if not ids:
            return {"flagged": 0, "ids": []}

        # 批量加 tags 'needs-review'
        for kid in ids:
            cur.execute("""
                UPDATE knowledge
                SET tags = array_append(COALESCE(tags, ARRAY[]::text[]), 'needs-review'),
                    updated = NOW()
                WHERE id = %s
            """, (kid,))
        conn.commit()
        logger.info(f"Layer2 软凭据抽查: {len(ids)} 条经验标 needs-review (calibration<0.5)")
        return {"flagged": len(ids), "ids": ids}
    except Exception as e:
        logger.warning(f"Layer2 flag_needs_review 失败: {e}")
        conn.rollback()
        return {"flagged": 0, "error": str(e)}
    finally:
        cur.close()
        conn.close()


def consolidate_expired():
    """Task407 项2: Mem0 式经验淘汰 v1（事后定时扫描）。

    淘汰规则（不物理删 D15，只标 deprecated=TRUE）：
    1. calibration_weight < 0.3 → 低质量淘汰
    2. superseded_by IS NULL + COALESCE(deprecated,FALSE)=FALSE（只处理活的）

    不做：
    - 30天无 search 命中（JSONB 扫描太重，v2）
    - content_hash 去重（add_knowledge 时 UNIQUE 索引已防重复）
    - LLM 判定四态（v2 工程）

    返回淘汰统计。
    """
    conn = _get_conn()
    cur = conn.cursor()
    try:
        # 低质量淘汰：calibration_weight < 0.3 + 未 deprecated
        cur.execute("""
            UPDATE knowledge
            SET deprecated = TRUE, updated = NOW()
            WHERE calibration_weight < 0.3
              AND COALESCE(deprecated, FALSE) = FALSE
              AND superseded_by IS NULL
        """)
        deprecated_count = cur.rowcount
        conn.commit()

        if deprecated_count > 0:
            logger.info(f"Layer2 Mem0淘汰: {deprecated_count} 条经验标 deprecated (calibration<0.3)")

        return {"deprecated": deprecated_count, "method": "calibration_weight<0.3"}
    except Exception as e:
        logger.warning(f"Layer2 consolidate_expired 失败: {e}")
        conn.rollback()
        return {"deprecated": 0, "error": str(e)}
    finally:
        cur.close()
        conn.close()


def run_full():
    """跑完整 Layer2 + 抽查 + 淘汰（供 crucible 后调用）。"""
    calib_result = run()
    review_result = flag_needs_review()
    consolidate_result = consolidate_expired()
    return {
        "calibration": calib_result,
        "needs_review": review_result,
        "consolidation": consolidate_result,
    }


def _self_test():
    """4 场景断言。"""
    print("=== layer2_calibrate self_test ===")

    # 场景1: 假阳性（推荐但失败）
    d = quantify_deviation("ABAA", "L", "failed")
    assert d == -1.0, f"假阳性应=-1.0, 得到 {d}"
    print(f"  ✅ 假阳性: deviation={d}")

    # 场景2: 正确（推荐且成功）
    d = quantify_deviation("ABAA", "L", "success")
    assert d == 1.0, f"正确应=1.0, 得到 {d}"
    print(f"  ✅ 正确: deviation={d}")

    # 场景3: 假阴性（没推荐但成功）
    d = quantify_deviation("DDDD", "H", "success")
    assert d == -0.5, f"假阴性应=-0.5, 得到 {d}"
    print(f"  ✅ 假阴性: deviation={d}")

    # 场景4: 不确定（pending）
    d = quantify_deviation("ABAA", "M", "pending")
    assert d == 0.0, f"pending应=0.0, 得到 {d}"
    print(f"  ✅ 不确定: deviation={d}")

    # 配对引擎跑通（查现有数据）
    pairs = pair_predictions_with_outcomes()
    print(f"  ✅ 配对引擎: {len(pairs)} 对")

    # run() 跑通
    result = run()
    print(f"  ✅ run(): {result}")

    print("=== self_test 全通过 ===")


if __name__ == "__main__":
    _self_test()
