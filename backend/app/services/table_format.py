"""table_format 领域服务：表格式治理与写入策略。

业务逻辑：
1. 表格式配置：表格式类型（Iceberg/Hudi/Delta）、主键列、分区列、写入模式
2. 表格式治理链路：lineage → table_format（血缘关系决定表格式选择）
3. 失败场景：表格式切换不兼容、写入模式冲突、合并引擎错误
4. 与上下游流水线：消费 lineage 血缘信息；消费 schema_govern 已核准的Schema版本，
   按核准版本的字段约束决定可写字段；已经开始写入的任务按原版本继续

跨模块数据流：
  lineage → table_format（血缘追踪结果决定表格式写入策略）
  schema_govern → table_format（已核准Schema版本决定可写字段与主键分区配置）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TableFormatService(BaseModuleService):
    MODULE = "table_format"
    ENTITY = "表格式配置"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["配置编号", "目标表", "表格式类型", "主键列", "分区列", "排序键列", "写入模式", "合并引擎"]
    REQUIRED_FIELDS = ["配置编号", "目标表", "表格式类型"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    PIPELINE = "table_format_governance"
    UPSTREAM_MODULES = ["lineage", "schema_govern"]
    PRODUCES_ARTIFACT = "表格式写入配置记录"

    SUPPORTED_FORMATS = ("iceberg", "hudi", "delta")

    def validate_format(self, entry_id: int) -> dict[str, Any]:
        """验证表格式配置的有效性。

        失败场景：
        - 表格式类型不支持
        - 写入模式与格式不兼容
        - 合并引擎错误
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}

        fmt = str(entry.get("表格式类型", "")).lower()
        write_mode = str(entry.get("写入模式", "append"))
        merge_engine = str(entry.get("合并引擎", "copy-on-write"))

        if fmt not in self.SUPPORTED_FORMATS:
            return {"ok": False, "reason": f"表格式 {fmt} 不在支持列表: {self.SUPPORTED_FORMATS}"}

        valid_modes = {
            "iceberg": ("append", "overwrite", "upsert"),
            "hudi": ("insert", "upsert", "insert_overwrite", "bulk_insert"),
            "delta": ("append", "overwrite", "merge"),
        }
        if write_mode not in valid_modes.get(fmt, ()):
            return {"ok": False, "reason": f"写入模式 {write_mode} 与格式 {fmt} 不兼容"}

        valid_engines = ("copy-on-write", "merge-on-read")
        if merge_engine not in valid_engines:
            return {"ok": False, "reason": f"合并引擎 {merge_engine} 不受支持"}

        return {"ok": True, "format": fmt, "write_mode": write_mode, "merge_engine": merge_engine}

    def resolve_write_strategy(self, entry_id: int) -> dict[str, Any]:
        """基于上游 lineage 和 schema_govern 信息解析写入策略。

        这是 lineage → table_format 和 schema_govern → table_format 贯通数据流的核心方法。
        可写字段由已核准（已发布）的 Schema 版本决定；已经开始写入的任务按原版本继续。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"配置未生效，当前状态: {entry.get('status')}"}

        table_name = str(entry.get("目标表", ""))
        fmt = str(entry.get("表格式类型", "iceberg")).lower()

        # 消费 lineage 的上下游关系
        lineage_info = store.lineage_downstream(table_name)
        upstream_count = sum(
            1 for edges in store._lineage_graph.values()
            for e in edges if e.get("downstream") == table_name
        )

        # 只消费 schema_govern 已核准（已发布）的 Schema 版本
        approved_schema = store.latest_published_schema(table_name)

        # 已经开始写入的任务按原版本继续
        active_task = store.active_write_task(table_name)
        if active_task is not None:
            schema_version = active_task.get("schema_version")
            constraints = active_task.get("constraints") or {}
            continues_original = True
        elif approved_schema is not None:
            schema_version = approved_schema.get("version")
            constraints = approved_schema.get("constraints") or {}
            continues_original = False
        else:
            schema_version = None
            constraints = {}
            continues_original = False

        # 基于血缘和已核准Schema决策写入策略
        strategy = {
            "format": fmt,
            "table": table_name,
            "lineage_upstream_count": upstream_count,
            "lineage_downstream_count": len(lineage_info),
            "schema_version": schema_version,
            "writable_fields": sorted(constraints),
            "field_constraints": constraints,
            "write_task_continues_original": continues_original,
        }

        # 根据血缘复杂度选择写入模式
        if upstream_count > 2 or len(lineage_info) > 2:
            strategy["recommended_write_mode"] = "upsert" if fmt == "iceberg" else "merge"
            strategy["reason"] = "血缘关系复杂，推荐使用upsert/merge模式"
        else:
            strategy["recommended_write_mode"] = "append"
            strategy["reason"] = "血缘关系简单，使用append模式"

        # 根据已核准Schema字段数量选择主键策略
        if constraints:
            if len(constraints) > 10:
                strategy["partition_recommendation"] = "按时间分区"
            else:
                strategy["partition_recommendation"] = "按主键分区"

        batch_id = f"TABLE_FMT_{entry_id}_{int(time.time())}"
        self.produce_artifact([strategy], batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            **strategy,
        }

    def begin_write_task(self, entry_id: int) -> dict[str, Any]:
        """开始写入任务：固定当前已核准的字段约束与注册版本组合。

        任务进行中即使有新版本发布，也按启动时核准的原版本继续写入。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"配置未生效，当前状态: {entry.get('status')}"}

        table_name = str(entry.get("目标表", ""))
        task = store.start_write_task(table_name)
        if task is None:
            return {"ok": False, "reason": f"表 {table_name} 无已核准的Schema版本，无法开始写入"}
        return {
            "ok": True,
            "table": table_name,
            "task_id": task.get("task_id"),
            "schema_version": task.get("schema_version"),
            "writable_fields": sorted(task.get("constraints") or {}),
        }

    def finish_write_task(self, entry_id: int) -> dict[str, Any]:
        """结束写入任务：释放版本固定，后续写入按最新核准版本执行。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        table_name = str(entry.get("目标表", ""))
        task = store.finish_write_task(table_name)
        if task is None:
            return {"ok": False, "reason": f"表 {table_name} 无进行中的写入任务"}
        return {
            "ok": True,
            "table": table_name,
            "finished_task": task.get("task_id"),
            "schema_version": task.get("schema_version"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_configs"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["active_write_tasks"] = len(store._active_write_tasks)
        return base