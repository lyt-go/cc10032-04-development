"""schema_govern 领域服务：Schema治理与版本演进管理。

业务逻辑：
1. Schema注册、版本号递增、逐字段向后兼容校验（旧记录能否被新定义读取）
2. 校验结论同步到注册清单（待启用候选）；确认注册时只发布经过核验的
   字段约束与注册版本组合
3. 并发登记冲突：另一候选先注册时按实际最新已发布版本重新核验，
   待启用定义记录实际核准依据（approval_basis）
4. 失败场景：金额列缩窄、新增无默认值必填列、删除已有字段、重复版本号
5. 与上下游流水线：消费 change_capture 变更；为 table_format 提供已核准Schema版本

跨模块数据流：
  change_capture → schema_govern（CDC变更驱动Schema演进）
  schema_govern → table_format（已核准Schema版本决定表格式可写字段）
"""
from __future__ import annotations

import time
from typing import Any

from app.schema_compat import check_backward_compatibility, normalize_fields
from app.services.base import BaseModuleService
from app.store import store


class SchemaGovernService(BaseModuleService):
    MODULE = "schema_govern"
    ENTITY = "Schema版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["主题名称", "Schema定义", "版本号", "兼容策略", "注册时间", "序列化格式", "字段列表", "演进历史"]
    REQUIRED_FIELDS = ["主题名称", "Schema定义", "版本号"]
    STATUS_ORDER = ["草稿", "待校验", "已注册", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "确认注册": "已注册", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "schema_evolution"
    UPSTREAM_MODULES = ["change_capture"]
    DOWNSTREAM_MODULES = ["table_format"]
    PRODUCES_ARTIFACT = "Schema演进版本链"

    def validate_compatibility(self, entry_id: int, new_field_spec: dict[str, Any]) -> dict[str, Any]:
        """逐字段校验新Schema版本的向后兼容性（旧记录能否被新定义读取）。

        判定维度：总位数与小数位数、可空性、默认值、字段重排。
        校验结论同步到注册清单（待启用候选），确认注册时按核准依据发布。

        失败场景：
        - 金额列缩窄（如 decimal(12,2) → decimal(10,2)）→ 不向后兼容
        - 新增无默认值必填列 → 旧记录无法补齐
        - 删除已有字段 → 破坏性变更
        - 重复版本号 → 拒绝注册
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}

        topic = str(entry.get("主题名称", ""))
        version_number = str(new_field_spec.get("version", "v1"))

        # 已发布版本号不可复用
        if any(v.get("version") == version_number
               for v in store.published_schema_versions(topic)):
            return {"ok": False, "reason": f"版本号 {version_number} 已存在，请使用新版本号"}

        new_def = {"fields": new_field_spec.get("fields", [])}
        base = store.latest_published_schema(topic)
        base_version = base.get("version") if base else None
        base_def = base.get("definition") if base else {"fields": []}
        report = check_backward_compatibility(base_def, new_def)

        # 校验结论同步到注册清单：登记为待启用候选版本
        store.add_schema_candidate(topic, new_def, version_number,
                                   base_version=base_version, compatibility=report)

        if not report["compatible"]:
            entry["abnormal"] = True
            entry["status"] = "待校验"
            return {"ok": False, "reason": f"兼容性校验失败: {report['reason']}",
                    "report": report, "base_version": base_version,
                    "candidate_version": version_number}

        return {"ok": True, "topic": topic, "version": version_number,
                "base_version": base_version, "compatibility": "BACKWARD",
                "fields": new_field_spec.get("fields", []), "report": report}

    def confirm_registration(self, entry_id: int, version: str | None = None) -> dict[str, Any]:
        """确认注册：只发布经过核验的字段约束与注册版本组合。

        并发场景下另一候选可能已先注册：按实际最新已发布版本重新核验，
        待启用定义记录实际核准依据（approval_basis）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}
        topic = str(entry.get("主题名称", ""))
        target_version = str(version or entry.get("版本号", ""))
        candidate = store.find_schema_candidate(topic, target_version)
        if candidate is None:
            return {"ok": False, "reason": f"主题 {topic} 版本 {target_version} 无待启用候选"}

        result = store.publish_schema(topic, target_version,
                                      expected_base=candidate.get("base_version"))
        record = store.find_schema_version(topic, target_version) or {}
        if not result["ok"]:
            entry["abnormal"] = True
            return {"ok": False, "reason": result["reason"],
                    "conflict": result.get("conflict", False),
                    "approval_basis": record.get("approval_basis"),
                    "report": result.get("report")}

        entry["status"] = "已注册"
        entry["pending"] = False
        entry["abnormal"] = False
        base_version = candidate.get("base_version")
        entry["演进历史"] = f"{base_version or '初始'}→{target_version}: 确认注册"
        return {"ok": True, "topic": topic, "version": target_version,
                "conflict": result.get("conflict", False),
                "approval_basis": record.get("approval_basis"),
                "constraints": record.get("constraints"),
                "report": result.get("report")}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """确认注册动作走治理路径：存在待启用候选时按核准依据发布。"""
        if action == "确认注册":
            entry = store.find(self.MODULE, entry_id)
            if entry is not None:
                candidate = store.find_schema_candidate(
                    str(entry.get("主题名称", "")), str(entry.get("版本号", "")))
                if candidate is not None:
                    result = self.confirm_registration(entry_id)
                    if result["ok"]:
                        message = f"Schema版本已确认注册（核准版本 {result['version']}）"
                        if result.get("conflict"):
                            message += "，已按实际最新版本重新核验"
                        return store.find(self.MODULE, entry_id), message
                    return None, result["reason"]
        return super().run_action(entry_id, action)

    def evolve_schema(self, entry_id: int, change_events: list[dict[str, Any]]) -> dict[str, Any]:
        """基于CDC变更事件驱动Schema演进。

        消费 change_capture 产生的变更事件，分析字段变更然后更新Schema注册表。
        这是 change_capture → schema_govern 贯通数据流的核心。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}
        if entry.get("status") != "已注册":
            return {"ok": False, "reason": f"Schema未注册，当前状态: {entry.get('status')}"}

        topic = str(entry.get("主题名称", ""))
        current_version = str(entry.get("版本号", "v1"))
        next_version = f"v{int(current_version.replace('v', '') or 1) + 1}"

        # 分析变更事件中的字段变化
        new_fields = {"id", "name", "ts", "status"}
        for event in change_events:
            if isinstance(event, dict) and event.get("type") == "INSERT":
                # 模拟从CDC事件中提取新字段
                new_fields.add("new_field_from_cdc")

        schema_def = {"fields": sorted(new_fields), "version": next_version}
        compat = self.validate_compatibility(entry_id, schema_def)
        if not compat["ok"]:
            return compat

        # 经过核验后走发布路径，保证注册清单里只有核验通过的版本组合
        publish = store.publish_schema(topic, next_version,
                                       expected_base=compat.get("base_version"))
        if not publish["ok"]:
            return {"ok": False, "reason": publish["reason"],
                    "report": publish.get("report")}

        # 创建新版本
        new_version = dict(entry)
        new_version["id"] = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        new_version["版本号"] = next_version
        new_version["Schema定义"] = str(schema_def)
        new_version["字段列表"] = str(sorted(new_fields))
        new_version["演进历史"] = f"{current_version}→{next_version}: 新增字段"
        new_version["status"] = "草稿"
        new_version["pending"] = True
        new_version["abnormal"] = False
        store.rows(self.MODULE).append(new_version)

        batch_id = f"SCHEMA_EVOLVE_{entry_id}_{int(time.time())}"
        self.produce_artifact([schema_def], batch_id)

        return {
            "ok": True,
            "topic": topic,
            "old_version": current_version,
            "new_version": next_version,
            "new_fields": list(new_fields),
            "source": "change_capture_events",
        }

    def emit_schema_for_table_format(self, topic: str) -> dict[str, Any]:
        """向 table_format 暴露当前已核准的Schema版本，用于决定写入策略"""
        published = store.published_schema_versions(topic)
        if not published:
            return {"ok": False, "reason": f"主题 {topic} 无已核准Schema"}
        latest = published[-1]
        return {
            "ok": True,
            "topic": topic,
            "latest_version": latest.get("version"),
            "definition": latest.get("definition"),
            "fields": [f["name"] for f in normalize_fields(latest.get("definition"))],
            "constraints": latest.get("constraints"),
            "downstream_consumer": "table_format",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["registry_topics"] = len(store._schema_registry)
        base["published_versions"] = sum(
            len(store.published_schema_versions(topic))
            for topic in store._schema_registry
        )
        base["pending_candidates"] = sum(
            1 for versions in store._schema_registry.values()
            for v in versions if v.get("status") == "candidate"
        )
        return base
