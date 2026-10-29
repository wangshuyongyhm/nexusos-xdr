#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.cli — 运维 CLI：update / doctor（task 1963 单5，design §7）。

入口：`python3 -m nexusos_xdr.cli <update|doctor>`。
（不声明 [project.scripts]：`nexus` 命令名归 nexusos-common 1965 向导 CLI
——1963/1965 并行撞名实证，双入口收编=语义合并设计，另单裁决。）

    python3 -m nexusos_xdr.cli update [--yes] [--index-url URL]   # 检查更新+迁移提示+升级
    python3 -m nexusos_xdr.cli doctor                             # 六服务/DB/守卫/版本对账

版本策略：四包 lockstep（统一版本号）；目录布局走独立 layout_version 轴
（nexusos_common.anchors.LAYOUT_VERSION，与 .nexus/config.toml [meta] 对账）。
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from importlib.metadata import PackageNotFoundError, version as _dist_version
from pathlib import Path

PACKAGES = ("nexusos-common", "nexusos-gate", "nexusos-mem", "nexusos-xdr")
PYPI_JSON = "https://pypi.org/pypi/{pkg}/json"
TESTPYPI_JSON = "https://test.pypi.org/pypi/{pkg}/json"

# 守卫 canonical 12（task 1963 单5）：守卫稳定化迁移对账基线
GUARDS = (
    "agent_prompt_guard", "broadcast_guard", "commit_msg_advisor",
    "completion_guard", "dispatch_waitcallback_guard", "dup_window_guard",
    "interaction_guard", "mem_first_guard", "oplog_guard",
    "selfcheck_evidence_guard", "state_transition_guard", "waitcallback_bg_guard",
)
GUARD_HOOKS_SUBDIR = "xdr/hooks"  # 稳定位：$NEXUS_HOME/xdr/hooks/<guard>.py

# 探活端点一律用 /health 便宜探针（search 等业务口延迟 3-8s 不宜作 liveness）
SERVICES = (
    ("xdr", "http://127.0.0.1:19998/session/active"),
    ("mem", "http://127.0.0.1:11537/health"),
    ("embed", "http://127.0.0.1:11436/health"),
    ("ocrx", "http://127.0.0.1:11438/health"),
    ("gateway", "http://127.0.0.1:4001/health"),
    ("console", "http://127.0.0.1:19900/health"),
)

DEFAULT_DSN = "postgresql://localhost/nexus_mem"


def _installed(pkg: str) -> str | None:
    try:
        return _dist_version(pkg)
    except PackageNotFoundError:
        return None


def _fetch_json(url: str, timeout: float = 6.0):
    req = urllib.request.Request(url, headers={"User-Agent": "nexus-cli/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ─────────────────────────── nexus update ───────────────────────────


def cmd_update(args) -> int:
    """检查更新（PyPI JSON API）→ layout_version 迁移提示 → one-liner 升级。"""
    rc = 0
    template = TESTPYPI_JSON if args.testpypi else PYPI_JSON
    index = args.index_url.rstrip("/") + "/{pkg}/json" if args.index_url else template

    latest: dict[str, str] = {}
    net_err: list[str] = []
    for pkg in PACKAGES:
        try:
            data = _fetch_json(index.format(pkg=pkg))
            latest[pkg] = data["info"]["version"]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                latest[pkg] = ""  # 该 index 无发布记录（本地视为最新）
            else:
                net_err.append(f"{pkg}: HTTP {e.code}")
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as e:
            net_err.append(f"{pkg}: {e}")

    if net_err:
        print("⚠️  索引查询失败（不影响本地对账）:")
        for e in net_err:
            print(f"   - {e}")

    outdated: list[str] = []
    for pkg in PACKAGES:
        cur = _installed(pkg)
        new = latest.get(pkg, "")
        if cur is None:
            print(f"❌ {pkg}: 未安装")
            rc = 1
        elif new and new != cur:
            outdated.append(pkg)
            print(f"⬆️  {pkg}: {cur} → {new}")
        else:
            suffix = f"（index 最新 {new}）" if new else "（index 无发布记录）"
            print(f"✅ {pkg} {cur} 已是最新 {suffix}")

    _layout_advice()

    if outdated:
        line = "pip install -U " + " ".join(outdated)
        print(f"\none-liner 升级：\n  {line}")
        if args.yes:
            import subprocess

            subprocess.run([sys.executable, "-m", "pip", "install", "-U", *outdated], check=False)
            print("升级执行完毕，建议跑 `nexus doctor` 自检。")
        else:
            print("（加 --yes 直接执行）")
    elif rc == 0:
        print("\n四包 lockstep 无落后项。")
    return rc


def _layout_advice() -> None:
    """layout_version 对账：.nexus/config.toml [meta] 落后 → 迁移提示。"""
    try:
        from nexusos_common.anchors import LAYOUT_VERSION
    except ImportError:
        print("⚠️  nexusos-common 未安装，跳过 layout_version 对账")
        return
    cfg = Path.home() / ".nexus" / "config.toml"
    declared = None
    if cfg.exists():
        for line in cfg.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s.startswith("layout_version"):
                declared = "".join(ch for ch in s.split("=", 1)[-1] if ch.isdigit())
                break
    if declared is None:
        print(f"✅ layout_version: 磁盘未声明（默认 1）= 包支持 {LAYOUT_VERSION}")
    elif int(declared) < LAYOUT_VERSION:
        print(f"⬆️  layout_version: 磁盘 {declared} < 包支持 {LAYOUT_VERSION}")
        print("    迁移提示：nexus migrate --dry-run 先看清单，确认后去掉 --dry-run 执行")
    else:
        print(f"✅ layout_version: 磁盘 {declared} >= 包支持 {LAYOUT_VERSION}")


# ─────────────────────────── nexus doctor ───────────────────────────


class _Report:
    def __init__(self) -> None:
        self.ok = 0
        self.warn = 0
        self.fail = 0

    def good(self, msg: str) -> None:
        self.ok += 1
        print(f"  ✅ {msg}")

    def bad(self, msg: str) -> None:
        self.fail += 1
        print(f"  ❌ {msg}")

    def caution(self, msg: str) -> None:
        self.warn += 1
        print(f"  ⚠️  {msg}")


def cmd_doctor(_args) -> int:
    r = _Report()
    print("== nexus doctor ==")

    _check_services(r)
    _check_db(r)
    _check_guards(r)
    _check_versions(r)
    _check_agents(r)

    print(f"\n结论: {r.ok} PASS / {r.warn} WARN / {r.fail} FAIL")
    return 1 if r.fail else 0


def _check_services(r: _Report) -> None:
    print("-- 服务 ×6 --")
    for name, url in SERVICES:
        try:
            _fetch_json(url, timeout=4.0)
            port = url.split(":")[2].split("/")[0]
            r.good(f"{name} :{port} 活")
        except Exception:
            port = url.split(":")[2].split("/")[0]
            r.bad(f"{name} :{port} 不可达")


def _check_db(r: _Report) -> None:
    import os

    print("-- DB --")
    try:
        import psycopg2
    except ImportError:
        r.caution("psycopg2 未安装，DB 对账跳过")
        return
    dsn = os.environ.get("NEXUS_PG_DSN", DEFAULT_DSN)
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
    except Exception as e:
        r.bad(f"连接失败 {dsn}: {e}")
        return
    with conn.cursor() as cur:
        cur.execute("SELECT extname FROM pg_extension WHERE extname='vector'")
        if cur.fetchall():
            r.good("pgvector 扩展在位")
        else:
            r.bad("pgvector 扩展缺失")
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND tablename IN ('knowledge','agents','credentials')"
        )
        have = {row[0] for row in cur.fetchall()}
        missing = {"knowledge", "agents", "credentials"} - have
        if missing:
            r.bad(f"核心表缺失: {sorted(missing)}")
        else:
            r.good("核心表 knowledge/agents/credentials 在位")
        try:
            cur.execute("SELECT key, value FROM _meta")
            meta = dict(cur.fetchall())
            ver = next((v for k, v in meta.items() if "schema" in str(k) and "version" in str(k)), None)
            if ver is not None:
                r.good(f"schema 版本登记: {ver}")
            else:
                r.caution(f"_meta 无 schema 版本键（现有键: {sorted(meta)[:5]}…）——1956 §5 置备制待登记")
        except Exception:
            r.caution("_meta 表不可读，schema 版本对账跳过")
    conn.close()


def _check_guards(r: _Report) -> None:
    print("-- 守卫 ×12 --")
    home = Path.home() / ".nexus"
    new_dir = home / GUARD_HOOKS_SUBDIR
    legacy_dirs = []
    try:
        from nexusos_common.anchors import vault_root

        vr = vault_root()
        if vr is not None:
            legacy_dirs.append(vr / "05-System" / "自动化脚本" / "nexusos_xdr" / "etc" / "hooks")
    except Exception:
        pass
    if new_dir.is_dir():
        base = new_dir
        where = str(new_dir)
    else:
        legacy = next((d for d in legacy_dirs if d.is_dir()), None)
        if legacy is None:
            r.bad(f"守卫既不在稳定位 {new_dir} 也无可达旧树")
            return
        base = legacy
        where = f"{legacy}（旧树易变路径——建议迁 {new_dir}，task 1963 守卫稳定化）"
        r.caution("守卫在旧树（结构性风险位）")
    found = [g for g in GUARDS if (base / f"{g}.py").is_file()]
    r.good(f"{len(found)}/12 在位 @ {where}")
    for g in GUARDS:
        if g not in found:
            r.bad(f"守卫缺失: {g}.py")


def _check_versions(r: _Report) -> None:
    print("-- 版本一致性 --")
    vers = {pkg: _installed(pkg) for pkg in PACKAGES}
    missing = [p for p, v in vers.items() if v is None]
    if missing:
        r.bad(f"未安装: {missing}")
        return
    distinct = sorted(set(vers.values()))
    if len(distinct) == 1:
        r.good(f"四包 lockstep = {distinct[0]}")
    else:
        r.bad(f"lockstep 破裂: {vers}")
    try:
        from nexusos_common.anchors import LAYOUT_VERSION

        r.good(f"layout_version 轴 = {LAYOUT_VERSION}")
    except ImportError:
        r.caution("nexusos-common 不可导入，layout_version 对账跳过")


def _check_agents(r: _Report) -> None:
    print("-- Agent 注册 --")
    try:
        data = _fetch_json("http://127.0.0.1:19998/session/active", timeout=4.0)
        agents = data.get("agents", data) if isinstance(data, dict) else data
        names = list(agents.keys()) if isinstance(agents, dict) else []
        if names:
            r.good(f"注册表可读，在线: {', '.join(names)}")
        else:
            r.caution("注册表可读但当前无人在线")
    except Exception as e:
        r.bad(f"Agent 注册表不可读: {e}")


# ─────────────────────────────── 入口 ───────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus", description="NEXUS 四包运维 CLI（update/doctor）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_up = sub.add_parser("update", help="检查更新 + 迁移提示 + one-liner 升级")
    p_up.add_argument("--yes", action="store_true", help="直接执行 pip install -U")
    p_up.add_argument("--testpypi", action="store_true", help="查询 test.pypi.org 而非正式 PyPI")
    p_up.add_argument("--index-url", default="", help="自定义 JSON API 索引基址（如本地镜像）")
    p_up.set_defaults(func=cmd_update)

    p_doc = sub.add_parser("doctor", help="六服务/DB/守卫/版本一致性对账")
    p_doc.set_defaults(func=cmd_doctor)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
