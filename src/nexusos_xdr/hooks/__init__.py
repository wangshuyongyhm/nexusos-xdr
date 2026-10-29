"""nexusos_xdr.hooks — ZCode hooks 守卫 canonical 源（task 1963 单5）。

12 守卫随包分发（canonical=本目录）；部署位=$NEXUS_HOME/xdr/hooks/
（稳定锚点，不再指向 vault 内易变路径——今日四事故的结构性根治）。
部署：cp 本目录/*.py $NEXUS_HOME/xdr/hooks/ 后同步 ~/.zcode/cli/config.json。
守卫全部 stdlib 自足，不 import 包内其他模块（部署副本可独立运行）。
"""
