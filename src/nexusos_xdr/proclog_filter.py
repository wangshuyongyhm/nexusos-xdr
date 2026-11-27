#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""proclog_filter.py — 过程日志写入上限闸（Task 2611230851）。

从标准输入读流，写到 <md>；累计超过上限后不再往 md 里写，只补一行说明，
同时把**收到的全部内容**（含上限以内那段）压进同目录的 `full-log.tui.log.gz`。
一个字节不丢：Markdown 有界，压缩文件保留全量。

用法：python3 -m nexusos_xdr.proclog_filter <md 路径> [上限字节，默认 262144]

约定（调用方依赖）：正常结束退出 0；任何异常往标准错误写一行、退出 **2** ——
派单管道用 `||` 接回「原样追加进文件」的老行为，退出码 2 就是那条回退信号。
"""
import gzip
import re
import sys
from pathlib import Path

DEFAULT_CAP = 262144
NOTE = "\n\n> 过程日志超过上限，此处截断；完整内容见同目录 full-log.tui.log.gz。\n"
GZ_NAME = "full-log.tui.log.gz"
CHUNK = 65536
# 2611230879：凭据号形状 cred_ + 16 位小写 hex（credential.py:95 credential_id 定义），
# 整段字符串替换；形状卡严（起止边界 + 字符集 + 长度），宁可漏抹不可错抹。
CRED_RE = re.compile(r"\bcred_[0-9a-f]{16}\b")
CARRY = 20  # 凭据号最大长度 21-1：块尾保留最多 20 字节 ASCII 尾巴与下一块拼接后替换


def _safe_carry(blob: bytes, n: int) -> bytes:
    """只保留块尾的 ASCII 连续段（凭据号是纯 ASCII；非 ASCII 多字节字符
    截断会损坏正文，绝不 carry——宁可漏抹）。"""
    tail = blob[-n:]
    i = len(tail)
    while i > 0 and tail[i - 1] < 128:
        i -= 1
    return tail[i:]


def _incomplete_utf8_tail(blob: bytes) -> bytes:
    """2611239903 项3（池#1353）：块尾未完的 UTF-8 多字节序列（留着与下一块拼接）。

    跨块汉字曾被 decode(replace) 切成替换字符（压缩文件比输入多 12 字节）。
    算法：从块尾回看最多 3 字节——撞到 ASCII 即边界安全；撞到前导字节则
    按它的期望长度判断序列是否完整，不完整就把这段尾巴留下来。
    """
    n = len(blob)
    for back in range(1, min(4, n + 1)):
        b = blob[n - back]
        if b < 0x80:              # ASCII：边界安全
            return b""
        if b >= 0xC0:             # 前导字节：期望长度 2/3/4
            need = 2 if b < 0xE0 else (3 if b < 0xF0 else 4)
            return blob[n - back:] if back < need else b""
    return b""                    # 全是延续字节（输入本身坏）：保持原样，下游 replace 兜底


def redact_credentials(text: str) -> str:
    """替换形如 cred_<16位小写hex> 的整段字符串；纯函数。"""
    return CRED_RE.sub("[凭据号已抹去]", text)


def _utf8_safe_head(data: bytes, room: int) -> bytes:
    """2611231129 乙（池#1234）：截断点回退到字符边界。

    原实现按字节切（`head = data[:room]`），一个汉字（3 字节）被切成两半时，
    md 产出不是合法 UTF-8（Task2611230888 实测）。从末尾最多回退 3 字节，
    取第一个能整体解码的前缀；回退不到就返回空（宁少不脏）。
    """
    head = data[:room]
    for cut in range(0, 4):
        cand = head[:len(head) - cut] if cut else head
        try:
            cand.decode("utf-8")
            return cand
        except UnicodeDecodeError:
            continue
    return b""


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if not argv:
        print("用法: python3 -m nexusos_xdr.proclog_filter <md 路径> [上限字节]", file=sys.stderr)
        return 2
    try:
        md = Path(argv[0])
        cap = int(argv[1]) if len(argv) > 1 else DEFAULT_CAP
        if cap <= 0:
            raise ValueError(f"上限必须为正数: {cap}")
    except Exception as e:
        print(f"[proclog_filter] 参数错误: {e}", file=sys.stderr)
        return 2

    gz_path = md.with_name(GZ_NAME)
    kept = bytearray()   # 上限以内已写进 md 的内容（最多 cap 字节，用于回填压缩文件）
    written = 0
    gz = None
    carry = b""  # 2611230879：跨块凭据号尾巴
    held = b""   # 2611231132：落在「上限-标记长度」之后的尾巴先攒着，流结束再定截不截断
    try:
        md.parent.mkdir(parents=True, exist_ok=True)
        with md.open("wb") as out, sys.stdin.buffer as src:
            while True:
                chunk = src.read(CHUNK)
                if not chunk:
                    # 2611231132：流结束时把「攒着的尾巴 + 凭据号 carry」一起按同一口径落盘——
                    # 总量 ≤ 上限就直接写完（不截断），否则按截断处理（正文 + 标记 ≤ 上限）。
                    _tail = b""
                    if carry:                # 流结束：flush 尾巴（redact 后写）
                        _tail = redact_credentials(carry.decode("utf-8", "replace")).encode("utf-8")
                        carry = b""
                    _pending = held + _tail
                    _note = NOTE.encode("utf-8")
                    if gz is not None:
                        if _pending:
                            gz.write(_pending)
                    elif written + len(_pending) <= cap:
                        if _pending:
                            out.write(_pending)
                            kept.extend(_pending)
                            written += len(_pending)
                    else:
                        _band = cap - len(_note) if cap > len(_note) else 0
                        _room = _band - written if _band > written else 0
                        _head = _utf8_safe_head(_pending, _room)
                        if _head:
                            out.write(_head)
                            written += len(_head)
                        # 2611231132：上限比标记还短时按剩余空间裁标记（保证总长 ≤ 上限）
                        out.write(_utf8_safe_head(_note, max(0, cap - written)))
                        gz = gzip.open(gz_path, "wb")
                        gz.write(bytes(kept))
                        gz.write(_head)
                        gz.write(_pending[len(_head):])
                        kept = bytearray()
                    held = b""
                    break
                blob = carry + chunk
                # 2611239903 项3：先留块尾未完的多字节字符（与下一块拼接），再算凭据号尾巴
                _utf8_pending = _incomplete_utf8_tail(blob)
                if _utf8_pending:
                    blob = blob[:len(blob) - len(_utf8_pending)]
                _cred_tail = _safe_carry(blob, CARRY)
                carry = _cred_tail + _utf8_pending
                emit = blob[:len(blob) - len(_cred_tail)]
                text = redact_credentials(emit.decode("utf-8", "replace"))
                data = text.encode("utf-8")
                if gz is not None:            # 已越界：后续全部进压缩文件（也先替换）
                    gz.write(data)
                    continue
                # 2611231132（池#1234 补刀）：放得下的判断与越界分支用同一口径——
                # 只要还可能写截断标记，就要为它留出位置。band = 可安全写满的上界
                # （cap - 标记长度）；落在 (band, cap] 之间的尾巴先攒进 held（内存上限一个块），
                # 流结束时若总量 ≤ cap 就直接写出（不必截断），否则按截断处理。
                # 旧写法 `if len(data) <= room` 没减去标记长度：上一块把已写字节推到
                # band 之内、下一块又整块放得下时，标记照样写出 → 总长超上限
                # （默认上限实测超 73 字节、扫 208 个上限值 100 个超出，最多超 94 字节）。
                note_bytes = NOTE.encode("utf-8")
                band = cap - len(note_bytes) if cap > len(note_bytes) else 0
                pending = held + data
                if written + len(pending) <= band:
                    out.write(pending)
                    kept.extend(pending)
                    written += len(pending)
                    held = b""
                    continue
                if written + len(pending) <= cap:
                    held = pending            # 先攒着，看流是否到此结束
                    continue
                # 确定要截断：band 以内写满 → 补说明 → 压缩文件补齐全量
                room = band - written
                head = _utf8_safe_head(pending, room)
                if head:
                    out.write(head)
                    written += len(head)
                # 2611231132：同上——标记按剩余空间裁，保证总长 ≤ 上限
                out.write(_utf8_safe_head(note_bytes, max(0, cap - written)))
                gz = gzip.open(gz_path, "wb")
                gz.write(bytes(kept))
                gz.write(head)
                gz.write(pending[len(head):])
                kept = bytearray()
                held = b""
        if gz is not None:
            gz.close()
            gz = None
    except Exception as e:                    # 不吞异常：写一行到 stderr + 退出码 2（回退信号）
        print(f"[proclog_filter] 失败: {type(e).__name__}: {e}", file=sys.stderr)
        try:
            if gz is not None:
                gz.close()
        except Exception:
            pass
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
