#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""notify_push — NEXUS 出向推送适配器（P1 通知层技术件，Task677）。

设计真源: task/649-agent-proxy-design/output/report.md §6（L158-213）。
通道定版（用户 08201125 手机端确认）: Server酱 主通道（已扫码绑定）+
TG 可插拔备用 + xdr alert 事件兜底。本单只做消息发出侧——接线 xdr 服务
属影子转正后的集成单；AGENT_PUSH_ENABLED 开关供集成单消费（§设计约束 9:
代理人自动推送转正前挡板，L1/L3 不受限）。

分级路由（649 §6.2 防轰炸）:
  L1 立即发（高风险不可逆待拍板/物理不可解）——静默时段(23:00-08:00)也发
  L2 30 分钟合并窗口（同窗 N 条合并一条编号清单，窗口到期发）；
     静默时段只存不推，08:00 合并晨报式清单
  L3 不推送仅入队列记录

失败降级（649 §6.5）: HTTP 非 200/超时 → 退避 1m/5m/15m ×3 → 切备用通道
→ 双败落 xdr alert 事件。任何路径不允许静默丢待决。

凭据: Server酱 sendkey 从 $NEXUS_HOME/secrets/push_serverchan.key 读（只读；
值不进代码/日志/异常输出——URL 打印前 key 段脱敏 ***）。

纯标准库（urllib），无第三方依赖。CLI:
  python3 -m nexusos_xdr.notify_push --self-test   # 全 mock 自检
  python3 -m nexusos_xdr.notify_push --send-test   # 真实发 1 条 L1（日限额慎用）
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行5）

# ── 常量（全部路径参数化：锚点经 nexusos_common，零本机硬编码）──
# NEXUS_XDR_NOTIFY_DIR 是目录级锚点（行5b/6 两文件共用，design §2.1）
NOTIFY_DIR = Path(os.environ["NEXUS_XDR_NOTIFY_DIR"]) if os.environ.get("NEXUS_XDR_NOTIFY_DIR") else subpath("xdr/notify")
QUEUE_FILE = NOTIFY_DIR / "push_queue.json"
XDR_EVENT_URL = "http://127.0.0.1:19998/session/event"

SERVERCHAN_API = "https://sctapi.ftqq.com/{key}.send"
TG_API = "https://api.telegram.org/bot{token}/sendMessage"

L2_WINDOW_SEC = 30 * 60          # L2 合并窗口
SILENCE_START, SILENCE_END = 23, 8   # 23:00-08:00（本地时间，陷阱 6）
BACKOFF_SCHEDULE = (60, 300, 900)    # 指数退避 1m/5m/15m（单测 monkeypatch 置 0）
MSG_TTL_HOURS = 24               # 回复有效期

# 🔴 影子转正挡板（任务书 §2 设计约束）：False 时代理人路径的 L2 推送被挡
# （返回 blocked，不入队不丢弃——调用方可决定降级 L3）。L1/L3 不受此开关限制。
# 集成单在影子转正后翻 True。
AGENT_PUSH_ENABLED = False

_NUM_EMOJI = ["1\u20e3", "2\u20e3", "3\u20e3", "4\u20e3", "5\u20e3",
              "6\u20e3", "7\u20e3", "8\u20e3", "9\u20e3"]


_LEVEL_LABEL = {"L1": "高风险", "L2": "待决", "L3": "记录"}


def _redact(text: str) -> str:
    """异常/日志输出脱敏：SCT key 段与 TG token 段替换 ***（陷阱 3，保留 .send 等后缀）。"""
    out = text
    if "SCT" in out:
        def _scrub(part):
            if part.startswith("SCT"):
                suffix = part[part.index("."):] if "." in part else ""
                return "***" + suffix
            return part
        out = "/".join(_scrub(p) for p in out.split("/"))
    return _redact_tg(out)


def _redact_tg(text: str) -> str:
    """TG token 段脱敏（/bot<token>/ → /bot***/）。"""
    if "/bot" in text:
        head, _, tail = text.partition("/bot")
        token = tail.split("/")[0]
        if token:
            return head + "/bot***/" + tail[len(token) + 1:]
    return text


def _http_error_detail(e: urllib.error.HTTPError) -> str:
    """把 HTTP 4xx/5xx 的响应体捞进错误信息（Task1588 可观测性修复）。

    HTTPError 默认只有一句 "HTTP Error 400: Bad Request"——真正的根因
    （如 Server酱配额超限 code=40001）在响应体里，不捞出来用户就永远
    只能看到"推送失败"却不知道为什么。返回串以 "4xx"/"5xx" 开头，
    deliver() 靠这个前缀判断要不要跳过重试。
    """
    body = e.read().decode("utf-8", "replace")
    e.close()
    try:
        api = json.loads(body)
        detail = f"api_code={api.get('code')}: {api.get('message', '')}"
    except ValueError:
        # 响应体不是 JSON（网关裸 HTML 之类）——原文截断保留线索，别让长页面刷屏
        detail = f"body={body[:200]!r}"
    kind = "4xx" if e.code < 500 else "5xx"
    return f"{kind} http={e.code} {detail}"


def format_body(blocker: str, background: str, rationale: str = "") -> str:
    """规范 body 构造器（649 §6.3 七字段中「卡点/背景/依据」三字段的载体）。

    push() 的 body 形参接受自由文本；用本函数生成则模板七字段标头齐全
    （背景超过 3 行取前 3 行——设计约束「背景≤3行」）。
    """
    bg_lines = [l for l in background.strip().splitlines() if l.strip()][:3]
    parts = [f"卡点：{blocker}", "背景：" + " / ".join(bg_lines)]
    if rationale:
        parts.append(f"依据：{rationale}")
    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════
# 通道插件（ChannelBase + 注册表——可扩展，649 §6.1）
# ═══════════════════════════════════════════════════════════════════

class ChannelBase:
    """通道插件基类。子类实现 send() 返回 (ok: bool, err: str|None)。"""

    name = "base"

    def available(self) -> bool:
        raise NotImplementedError

    def send(self, title: str, desp: str) -> tuple:
        raise NotImplementedError


class ServerChan(ChannelBase):
    """Server酱（主通道，用户已扫码绑定）。POST 表单 title+desp（desp=markdown）。

    成功判据: HTTP 200 且响应 JSON code == 0。
    """

    name = "serverchan"

    def __init__(self, key_file: Path | None = None):
        # 调用时解析（design §2.1 规范）：默认参数在 def 时绑定会让
        # NEXUS_XDR_PUSH_KEY_FILE/测试 monkeypatch 失效（1960 实证）
        self._key_file = key_file or Path(os.environ.get("NEXUS_XDR_PUSH_KEY_FILE")
                                          or subpath("secrets/push_serverchan.key"))
        self._key = None

    def available(self) -> bool:
        try:
            self._key = self._key_file.read_text(encoding="utf-8").strip()
        except OSError:
            self._key = None
        return bool(self._key)

    def send(self, title: str, desp: str) -> tuple:
        if not self.available():
            return False, "sendkey 文件缺失或为空"
        url = SERVERCHAN_API.format(key=self._key)
        data = urllib.parse.urlencode({"title": title, "desp": desp}).encode()
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, data=data), timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
                if payload.get("code") == 0:
                    return True, None
                return False, _redact(f"api code={payload.get('code')}")
        except urllib.error.HTTPError as e:
            # 必须放在 URLError 分支前面：HTTPError 是 URLError 的子类，
            # 放后面永远轮不到。4xx 响应体里才有真根因（如配额 40001）
            return False, _redact(_http_error_detail(e))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            return False, _redact(f"{type(e).__name__}: {e}")


class Telegram(ChannelBase):
    """TG 备用通道。凭据从 env 读（NEXUS_TG_BOT_TOKEN / NEXUS_TG_CHAT_ID）；
    缺失=通道优雅禁用（不报错不崩溃）。"""

    name = "telegram"

    def __init__(self):
        self._token = os.environ.get("NEXUS_TG_BOT_TOKEN", "")
        self._chat = os.environ.get("NEXUS_TG_CHAT_ID", "")

    def available(self) -> bool:
        return bool(self._token and self._chat)

    def send(self, title: str, desp: str) -> tuple:
        if not self.available():
            return False, "TG 凭据缺失（env 未配置）"
        url = TG_API.format(token=self._token)
        data = json.dumps({
            "chat_id": self._chat, "text": f"{title}\n\n{desp}",
            "parse_mode": "Markdown",
        }).encode()
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(
                        url, data=data, headers={"Content-Type": "application/json"}),
                    timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
                if payload.get("ok"):
                    return True, None
                return False, _redact(f"tg api ok={payload.get('ok')}")
        except urllib.error.HTTPError as e:
            # 同 ServerChan：TG 启用后防同款盲区（响应体根因被丢弃）
            return False, _redact(_http_error_detail(e))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            return False, _redact(f"{type(e).__name__}: {e}")


# 注册表（集成单可注入新通道: REGISTRY["sms"] = SmsChannel）
REGISTRY = {"serverchan": ServerChan, "telegram": Telegram}
CHANNEL_ORDER = ("serverchan", "telegram")


# ═══════════════════════════════════════════════════════════════════
# 队列持久化（L2 窗口 + L3 记录 + 静默晨报缓存）
# ═══════════════════════════════════════════════════════════════════

def _queue_load(path: Path = QUEUE_FILE) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"l2_window": None, "l2_items": [], "silent_items": [], "l3_log": []}


def _queue_save(state: dict, path: Path = QUEUE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


def _in_silence(now=None) -> bool:
    """静默时段 23:00-08:00（本地时间，陷阱 6）。"""
    lt = time.localtime(now if now is not None else time.time())
    return lt.tm_hour >= SILENCE_START or lt.tm_hour < SILENCE_END


# ═══════════════════════════════════════════════════════════════════
# 消息模板（649 §6.3 七字段）+ 选项渲染
# ═══════════════════════════════════════════════════════════════════

def render_options(options) -> str:
    """编号选项渲染：1️⃣2️⃣3️⃣…（元素为 str 或 {text, note}，最多 9 项）。"""
    if not options:
        return ""
    lines = []
    for i, opt in enumerate(options[:9]):
        if isinstance(opt, dict):
            text, note = opt.get("text", ""), opt.get("note", "")
            entry = text + (f"（{note}）" if note else "")
        else:
            entry = str(opt)
        lines.append(f"{_NUM_EMOJI[i]} {entry}")
    return "\n".join(lines)


def render_message(level: str, title: str, body: str, options, msg_id: str) -> str:
    """七字段模板：任务/卡点+背景+依据（body 载体）/编号选项/回复格式/24h 有效期。

    body 传自由文本时原样渲染；用 format_body() 构造则字段标头齐全。
    """
    head = f"【NEXUS 待决 #{msg_id}】（{level}/{_LEVEL_LABEL.get(level, '')}）"
    parts = [head, f"任务：{title}", body]
    opts = render_options(options)
    if opts:
        parts.append(f"选项：{opts.replace(chr(10), ' ')}")
        n = len(options[:9])
        replies = " / ".join(f"{msg_id}-{i + 1}" for i in range(n))
        parts.append(f"回复：{replies}（{MSG_TTL_HOURS}h 内有效）")
    else:
        parts.append(f"回复：见 Console 决策队列（{MSG_TTL_HOURS}h 内有效）")
    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════
# 降级链（649 §6.5）: 主通道退避×3 → 备用 → xdr alert 兜底
# ═══════════════════════════════════════════════════════════════════

def _xdr_alert(detail: str) -> bool:
    """双通道全败的最终落点：POST xdr /session/event（type=alert）。

    不允许静默丢待决——本函数自身失败也打印 stderr（最终可见性）。
    Task1568 (1564 池建议①): 前置注册态自查——agent 未注册/失效时不再 400 盲推，
    降级用 nexus-monitor（注册名，Bug11 惯例）+ detail 前缀归因。
    """
    agent = "notify_push"
    try:
        import sys as _sys
        # src/ 在 parents[1]（nexusos-xdr/src/nexusos_xdr/notify_push.py）——
        # 未 editable 安装的环境也能找到 nexusos_xdr 包；已安装时 insert 是无害冗余
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from nexusos_xdr.session_state import validate_agent
        ok, reason = validate_agent(agent)
        if not ok:
            agent = "nexus-monitor"
            detail = f"[notify_push-fallback] {detail}"
            print(f"[notify_push] agent 'notify_push' 注册态失效({reason})，"
                  f"兜底降级 {agent}", file=sys.stderr)
    except Exception as e:
        # 自查链路自身异常 → 保守降级（注册名保兜底可达）
        agent = "nexus-monitor"
        detail = f"[notify_push-fallback] {detail}"
        print(f"[notify_push] 注册态自查异常({e})，兜底降级 {agent}", file=sys.stderr)
    body = json.dumps({
        "agent": agent, "type": "alert",
        "detail": detail[:2000], "target": "",
    }).encode()
    try:
        with urllib.request.urlopen(
                urllib.request.Request(
                    XDR_EVENT_URL, data=body,
                    headers={"Content-Type": "application/json"}), timeout=5) as resp:
            return resp.status == 200
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"[notify_push] xdr alert 兜底也失败（不丢弃，stderr 留痕）: "
              f"{_redact(str(e))}", file=sys.stderr)
        return False


def _sleep(sec: float) -> None:   # 单测 monkeypatch 锚点
    time.sleep(sec)


def deliver(title: str, desp: str) -> dict:
    """带降级链的投递：CHANNEL_ORDER 逐通道，单通道内退避×BACKOFF_SCHEDULE。

    返回 {"delivered": bool, "channel": str|None, "fallback_alert": bool, "errors": [...]}。
    """
    errors = []
    for chan_name in CHANNEL_ORDER:
        chan = REGISTRY[chan_name]()
        if not chan.available():
            errors.append(f"{chan_name}: disabled")
            continue
        attempt_errors = []
        for attempt in range(len(BACKOFF_SCHEDULE) + 1):
            ok, err = chan.send(title, desp)
            if ok:
                return {"delivered": True, "channel": chan_name,
                        "fallback_alert": False, "errors": errors + attempt_errors}
            attempt_errors.append(f"{chan_name}#{attempt + 1}: {err}")
            if err and err.startswith("4xx"):
                # 4xx 是确定性失败——配额没重置前重发几次都是 40001，
                # 白烧三次退避等待；直接换下一通道（5xx/网络错仍照常重试）
                break
            if attempt < len(BACKOFF_SCHEDULE):
                _sleep(BACKOFF_SCHEDULE[attempt])
        errors.extend(attempt_errors)
    # 双通道全败 → xdr alert 兜底（不静默丢）
    fallback = _xdr_alert(f"notify_push 双通道全败: {title} | " + " ; ".join(errors[-3:]))
    return {"delivered": False, "channel": None,
            "fallback_alert": fallback, "errors": errors}


# ═══════════════════════════════════════════════════════════════════
# 主接口
# ═══════════════════════════════════════════════════════════════════

def _flush_l2_window(state: dict) -> dict:
    """把过期 L2 窗口的内容合并成一条编号清单发出（跨窗自然拆条）。"""
    win = state.get("l2_window")
    items = state.get("l2_items", [])
    if not items:
        state["l2_window"] = None
        return state
    if win is not None and time.time() - win < L2_WINDOW_SEC:
        return state   # 窗口未到期
    lines = ["【NEXUS L2 合并清单】"]
    for i, it in enumerate(items[:9]):
        lines.append(f"{_NUM_EMOJI[i]} [{it['level']}] {it['title']}")
        if it.get("body"):
            lines.append(f"   {it['body'].splitlines()[0][:60]}")
    lines.append(f"（{len(items)} 条合并，详情见各单消息体）")
    result = deliver("NEXUS L2 合并清单", "\n".join(lines))
    state["l2_window"] = None
    state["l2_items"] = []
    state["last_flush_result"] = result
    return state


def push(level: str, title: str, body: str, options=None,
         via_agent: bool = False, queue_path: Path = QUEUE_FILE) -> dict:
    """推送入口（649 §6 接口）。level ∈ {"L1","L2","L3"}。

    Args:
        level: L1 立即 / L2 30min 合并窗口 / L3 仅记录
        title/body: 任务标题与正文（body 建议 format_body() 构造七字段）
        options: 编号选项列表（str 或 {text, note}）
        via_agent: 代理人路径标记——True 且 AGENT_PUSH_ENABLED=False 时
            L2 被挡板拦截（影子转正前语义，返回 blocked；不丢不推）。
        queue_path: 队列文件（测试注入 tmp_path）
    Returns:
        {"status": sent|queued|recorded|blocked, "msg_id", ...投递详情}
    """
    if level not in ("L1", "L2", "L3"):
        raise ValueError(f"level 必须是 L1/L2/L3，得到 {level!r}")
    msg_id = uuid.uuid4().hex[:4].upper()

    if via_agent and level == "L2" and not AGENT_PUSH_ENABLED:
        return {"status": "blocked", "msg_id": msg_id,
                "reason": "AGENT_PUSH_ENABLED=False（影子转正挡板，L2 代理路径不推）"}

    state = _queue_load(queue_path)
    try:
        if level == "L3":
            state.setdefault("l3_log", []).append(
                {"msg_id": msg_id, "title": title, "ts": time.time()})
            _queue_save(state, queue_path)
            return {"status": "recorded", "msg_id": msg_id}

        if level == "L1":
            desp = render_message("L1", title, body, options, msg_id)
            result = deliver(f"[L1] {title}", desp)
            return {"status": "sent" if result["delivered"] else "fallback",
                    "msg_id": msg_id, "deliver": result}

        # L2：静默时段只存（晨报合并），否则进 30min 窗口
        state = _flush_l2_window(state)   # 先冲已过期窗口（跨窗拆条）
        entry = {"msg_id": msg_id, "level": "L2", "title": title, "body": body}
        if _in_silence():
            state.setdefault("silent_items", []).append(entry)
            state["pending_digest"] = True
            _queue_save(state, queue_path)
            return {"status": "queued_silent", "msg_id": msg_id,
                    "note": "静默时段(23-08)只存不推，08:00 晨报合并"}
        state.setdefault("l2_items", []).append(entry)
        if state.get("l2_window") is None:
            state["l2_window"] = time.time()
        _queue_save(state, queue_path)
        return {"status": "queued", "msg_id": msg_id,
                "window_items": len(state["l2_items"]),
                "note": f"L2 合并窗口 {L2_WINDOW_SEC // 60}min，到期合并发"}
    finally:
        pass


def morning_digest(queue_path: Path = QUEUE_FILE) -> dict:
    """08:00 晨报：静默时段积压合并一条清单发出（集成单定时器调用；
    手动调用幂等——无积压时 no-op）。"""
    state = _queue_load(queue_path)
    silent = state.get("silent_items", [])
    if not silent:
        return {"status": "noop", "sent": 0}
    lines = ["【NEXUS 晨报（静默时段积压）】"]
    for i, it in enumerate(silent[:9]):
        lines.append(f"{_NUM_EMOJI[i]} {it['title']}")
    lines.append(f"（共 {len(silent)} 条）")
    result = deliver("NEXUS 晨报", "\n".join(lines))
    state["silent_items"] = []
    state.pop("pending_digest", None)
    state["last_digest_result"] = result
    _queue_save(state, queue_path)
    return {"status": "sent" if result["delivered"] else "fallback",
            "sent": len(silent), "deliver": result}


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def _self_test() -> int:
    """全 mock 自检：不发任何真实网络请求。"""
    import unittest.mock as mock

    ok_chan = mock.Mock(spec=ServerChan)
    ok_chan.available.return_value = True
    ok_chan.send.return_value = (True, None)
    with mock.patch.dict(REGISTRY, {"serverchan": lambda: ok_chan}), \
         mock.patch.dict("os.environ", {}, clear=False):
        r1 = push("L1", "自检-任务", format_body("自检卡点", "自检背景行1\n行2", "自检依据"),
                  options=["甲", "乙"], queue_path=Path("/tmp/_np_selftest.json"))
        r3 = push("L3", "自检-记录", "x", queue_path=Path("/tmp/_np_selftest.json"))
    msg = render_message("L1", "t", format_body("k", "b", "r"), ["o1", {"text": "o2", "note": "n"}], "AB12")
    fields = all(k in msg for k in ("任务：", "卡点：", "背景：", "依据：", "选项：", "回复：", "有效"))
    checks = [
        ("L1 sent", r1["status"] == "sent"),
        ("L3 recorded", r3["status"] == "recorded"),
        ("七字段模板", fields),
        ("选项渲染", "1\u20e3" in msg and "（n）" in msg),
        ("脱敏", _redact("https://sctapi.ftqq.com/SCT123abc.send") == "https://sctapi.ftqq.com/***.send"),
    ]
    for name, ok in checks:
        print(f"  {'✅' if ok else '❌'} {name}")
    Path("/tmp/_np_selftest.json").unlink(missing_ok=True)
    return 0 if all(ok for _, ok in checks) else 1


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if "--self-test" in argv:
        return _self_test()
    if "--send-test" in argv:
        result = push("L1", "notify_push 自检",
                      format_body("通道连通性验证", "本条为全任务唯一真实发送", "Task677 验收 3"))
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result["status"] == "sent" else 1
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
