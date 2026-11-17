"""nexusos_xdr.hooks — ZCode hooks 守卫 canonical 源（task 1963 单5）。

12 守卫随包分发（canonical=本目录）；部署位=$NEXUS_HOME/xdr/hooks/
（稳定锚点，不再指向 vault 内易变路径——今日四事故的结构性根治）。
部署：cp 本目录/*.py $NEXUS_HOME/xdr/hooks/ 后同步 ~/.zcode/cli/config.json。
守卫以 stdlib 为主，但非全部 stdlib 自足——三个可选 import（均 try/except
兜底，缺包时降级不炸，部署副本仍可独立运行）：
- commit_msg_advisor: from nexusos_xdr._vault_env import VAULT_ROOT（:23）
- commit_msg_advisor: from nexus.shared import SharedKnowledge（:186，MEM 上报）
- state_transition_guard: from nexusos_xdr.session_state import archive_path_today（:86）
"""
