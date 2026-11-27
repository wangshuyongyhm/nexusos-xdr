"""task_aliases — 同一任务的各种任务号写法（2611230886）。

背景：统一检索命令的 --task 是自由文本原样透传；执行者按老习惯带数字任务号
（如 2611230880）检索时，套用记录的 task_id 列存的是数字任务号——完成判定、
签收接口、完成门禁按消息总线任务号查不到它们。本模块给出同一任务的别名集合，
三处读取端统一用 task_id = ANY(别名) 查询。

边界（设计接受）：同一个数字任务号被取消后重派会出现两个消息总线任务号，
数字任务号记录会被两边都匹配到——近似接受，不报错。
"""
import os, re, json, glob


def task_id_aliases(task_id: str, task_path: str = "") -> list:
    """同一任务的各种任务号写法，去重去空。

    来源：①传入的消息总线任务号；②任务路径里的数字任务号（task/2611230880-名/…）；
    ③任务书 frontmatter 的 task_id（任务路径或同目录 task.md 存在才读，读不到不报错）。
    没传任务路径时从台账自取，自取失败只有 ①。
    """
    aliases = set()
    if task_id:
        aliases.add(task_id)

    def _add_from_path(p):
        m = re.search(r"task/(\d+)[-/]", p)
        if m:
            aliases.add(m.group(1))
        base = os.path.dirname(os.path.dirname(p))
        tm = os.path.join(base, "input", "task.md")
        if os.path.isfile(tm):
            try:
                txt = open(tm, encoding="utf-8", errors="replace").read()
                m2 = re.search(r'^task_id:\s*["\']?(\d+)', txt, re.M)
                if m2:
                    aliases.add(m2.group(1))
            except Exception:
                pass

    if task_path:
        _add_from_path(task_path)
    elif task_id:
        try:
            for f in glob.glob(os.path.expanduser("~/.nexus/xdr/tasks/*.json")):
                try:
                    d = json.load(open(f, encoding="utf-8"))
                except Exception:
                    continue
                if d.get("task_id") == task_id and d.get("task_path"):
                    _add_from_path(d["task_path"])
                    break
        except Exception:
            pass
    return [a for a in aliases if a]
