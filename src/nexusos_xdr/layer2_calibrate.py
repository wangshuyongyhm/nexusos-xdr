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
CALIB_MIN = 0.1
CALIB_MAX = 2.0
# 2611230871：退役阈值与下限拉开——权重可低于 0.4，但退役要满足三个条件（见 consolidate_expired）
RETIRE_WEIGHT = 0.4
MIN_RESULTS_FOR_RETIRE = 5
# 2611192127 #973: inferred_apply（expire 推断套用）弱正档位——推导见 quantify_deviation
INFERRED_WEAK_POSITIVE = 0.2
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


def quantify_deviation(pred_seq: str, pred_uncertainty: str, outcome: str) -> float | None:
    """偏差量化。

    返回 None = 显式跳过（无校准信号）：uncertain/pending/未知枚举。
    调用方 run() 对 None 计数留痕，不再混入 0.0 被静默丢弃（2611192127 #973）。

    偏差值：
      -1.0 = 假阳性（推荐套用但 outcome=failed）
      -0.5 = 假阴性（没推荐但 outcome=success）
      +1.0 = 正确（outcome=success + 推荐 L）
      +0.5 = 正确偏保守（outcome=success + 推荐 M）
      +0.3 = 弱正（outcome=partial，Task532 P-s2）
      +0.2 = 弱正·推断套用（outcome=inferred_apply，2611192127 #973）
      None = 显式跳过（uncertain/pending/未知枚举）

    inferred_apply=+0.2 推导：success 族实测偏差恒 0.5（baseline-before.json
    544 条，std=0）；inferred 由 expire_stale_pendings 按命中≥INFER_MIN_HITS
    推断产出，证据强度低于真实回填 → 取 0.5×0.4 保守系数=0.2，同时满足三条
    硬约束：严格低于 partial 0.3、严格大于 0、0.1 步进档内最大的子 partial 档。
    weight 变动经 update_calibration_weight 现有 [0.3,2.0] 夹逼自然收敛，
    不动阈值逻辑（收敛速度仿真实证见 2611192127 output/files/adv-clamp-sim.json）。

    pred 推荐 = uncertainty in (L, M)（低中不确定=倾向推荐）
    pred 不推荐 = uncertainty = H（高不确定=倾向不推荐）

    输入归一化：outcome 首尾空白剔除；uncertainty 空白剔除+大写化（" m "→"M"）。
    大小写混合 outcome（"Success"）不升格为 success——按未知枚举显式跳过
    （保守：脏值不产生校准信号，但计数留痕可观测）。
    """
    pred_uncertainty = (pred_uncertainty or "").strip().upper() or None
    outcome = (outcome or "").strip()
    is_recommended = pred_uncertainty in ("L", "M", None)
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
    if outcome == "inferred_apply":
        return INFERRED_WEAK_POSITIVE  # 2611192127 #973: 推断套用=弱正（推导见 docstring）
    return None  # uncertain/pending/未知枚举：显式跳过，由 run() 计数留痕


def compute_new_weight(current: float, deviation: float, weak_mode: bool = True) -> float:
    """2611230871 抽纯函数：校准权重单步更新（便于测试连续负偏差跌破 0.4）。"""
    multiplier = 0.1 if weak_mode else 0.3
    adjustment = deviation * multiplier
    return max(CALIB_MIN, min(CALIB_MAX, current + adjustment))


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
        new_weight = compute_new_weight(current, deviation, weak_mode)

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
    """批量校准：跑配对→量化→更新。返回统计。

    2611192127 #973: inferred_apply 进弱正轨道（+0.2）；uncertain/pending/
    未知 outcome 显式 skip 计数（quantify_deviation 返回 None），不再 0.0 静默丢。
    """
    pairs = pair_predictions_with_outcomes()
    pair_count = len(pairs)

    # 冷启动判断
    if pair_count < COLD_START_NO_CALIB:
        logger.info(f"Layer2: 配对数 {pair_count} < {COLD_START_NO_CALIB}, 冷启动不校准")
        return {"pairs": pair_count, "calibrated": 0, "mode": "cold_start_no_calib",
                "inferred_calibrated": 0, "uncertain_skipped": 0}

    weak_mode = pair_count < COLD_START_WEAK
    mode = "weak" if weak_mode else "normal"
    logger.info(f"Layer2: 配对数 {pair_count}, {'弱' if weak_mode else '正常'}校准模式")

    calibrated = 0
    inferred_calibrated = 0
    uncertain_skipped = 0
    for p in pairs:
        deviation = quantify_deviation(p["pred_seq"], p["pred_uncertainty"], p["outcome"])
        if deviation is None:
            uncertain_skipped += 1
            logger.info(f"Layer2: exp[{p['exp_id']}] outcome={p['outcome']!r} 无校准信号，显式跳过")
            continue
        result = update_calibration_weight(p["exp_id"], deviation, weak_mode=weak_mode)
        if result is not None:
            calibrated += 1
            if p["outcome"] == "inferred_apply":
                inferred_calibrated += 1

    return {"pairs": pair_count, "calibrated": calibrated, "mode": mode,
            "inferred_calibrated": inferred_calibrated,
            "uncertain_skipped": uncertain_skipped}


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


def consolidate_expired(dry_run: bool = True):
    """Task407 项2: Mem0 式经验淘汰 v2（2611230871 改造：三条件 + 预演默认）。

    淘汰三条件（缺一不可）：
    1. calibration_weight < RETIRE_WEIGHT(0.4)
    2. 真实结果数（agent_activity 里 outcome∈success/failed/partial）≥ MIN_RESULTS_FOR_RETIRE(5)
    3. COALESCE(deprecated,FALSE)=FALSE 且 superseded_by IS NULL（只处理活的）

    退役动作：标 deprecated=TRUE + agent_activity 追加 retire_by_calib 留痕（权重/结果数/规则）。
    恢复办法：UPDATE knowledge SET deprecated=FALSE, calibration_weight=1.0, updated=NOW() WHERE id=…；
              再追加一行 agent_activity（action=unretire, detail 记恢复原因）——可逆。

    dry_run=True（默认）：只算不写，输出会退条数、结果数分布、前 20 条清单。
    """
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT k.id, k.title, k.calibration_weight,
                   (SELECT count(*) FROM agent_activity a
                    WHERE a.target_id = k.id AND a.action = 'apply_attempt'
                      AND a.outcome IN ('success','failed','partial')) AS real_results
            FROM knowledge k
            WHERE k.calibration_weight < %s
              AND COALESCE(k.deprecated, FALSE) = FALSE
              AND k.superseded_by IS NULL
        """, (RETIRE_WEIGHT,))
        candidates = [{"id": r[0], "title": r[1], "weight": float(r[2] or 1.0),
                       "results": int(r[3] or 0)} for r in cur.fetchall()]
        eligible = [c for c in candidates if c["results"] >= MIN_RESULTS_FOR_RETIRE]
        ineligible_low_results = [c for c in candidates if c["results"] < MIN_RESULTS_FOR_RETIRE]

        if not dry_run:
            for c in eligible:
                cur.execute(
                    "UPDATE knowledge SET deprecated = TRUE, updated = NOW() WHERE id = %s",
                    (c["id"],))
                cur.execute(
                    "INSERT INTO agent_activity (agent_id, action, target_type, target_id, detail) "
                    "VALUES ('auto', 'retire_by_calib', 'knowledge', %s, %s::jsonb)",
                    (c["id"],
                     '{"rule": "2611230871", "action_short": "retire_by_calib", "weight": ' + str(c["weight"]) +
                     ', "results": ' + str(c["results"]) +
                     ', "note": "权重低于退役阈值且结果数达标，标废弃可逆"}'))
            conn.commit()

        if eligible:
            logger.info(f"Layer2 退役预演/执行: 会退 {len(eligible)} 条（结果数不足被挡 {len(ineligible_low_results)} 条）")
        return {
            "dry_run": dry_run,
            "candidates_below_threshold": len(candidates),
            "would_retire": len(eligible),
            "blocked_low_results": len(ineligible_low_results),
            "top20": [{"id": c["id"], "title": (c["title"] or "")[:50],
                       "weight": round(c["weight"], 3), "results": c["results"]}
                      for c in sorted(eligible, key=lambda x: x["weight"])[:20]],
        }
    except Exception as e:
        logger.warning(f"Layer2 consolidate_expired 失败: {e}")
        conn.rollback()
        return {"dry_run": dry_run, "error": str(e)}
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
    """场景断言（含 2611192127 #973 新语义）。"""
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

    # 场景4: pending 显式跳过（2611192127 #973：原 0.0 静默丢改 None 计数）
    d = quantify_deviation("ABAA", "M", "pending")
    assert d is None, f"pending应=None(显式跳过), 得到 {d}"
    print(f"  ✅ pending 显式跳过: deviation={d}")

    # 场景5: inferred_apply 弱正（2611192127 #973）
    d = quantify_deviation("ABAA", "M", "inferred_apply")
    assert d == INFERRED_WEAK_POSITIVE, f"inferred_apply应={INFERRED_WEAK_POSITIVE}, 得到 {d}"
    print(f"  ✅ inferred_apply 弱正: deviation={d}")

    # 场景6: uncertain 显式跳过（2611192127 #973 核心目标②）
    d = quantify_deviation("ABAA", "M", "uncertain")
    assert d is None, f"uncertain应=None(显式跳过), 得到 {d}"
    print(f"  ✅ uncertain 显式跳过: deviation={d}")

    # 场景7: 脏值（大小写混合）不升格，显式跳过
    d = quantify_deviation("ABAA", "M", "Success")
    assert d is None, f"脏值应=None(显式跳过), 得到 {d}"
    print(f"  ✅ 脏值显式跳过: deviation={d}")

    # 配对引擎跑通（查现有数据）
    pairs = pair_predictions_with_outcomes()
    print(f"  ✅ 配对引擎: {len(pairs)} 对")

    # run() 跑通
    result = run()
    print(f"  ✅ run(): {result}")

    print("=== self_test 全通过 ===")


if __name__ == "__main__":
    _self_test()
