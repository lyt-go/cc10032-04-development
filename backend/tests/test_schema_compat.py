"""Schema治理：逐字段向后兼容规则与注册发布治理测试。

覆盖：
1. decimal 总位数/小数位数/整数位：decimal(12,2) 缩窄到 decimal(10,2) 拒绝，
   放宽到 decimal(14,2) 通过
2. 可空性、默认值、字段重排的逐字段判定
3. 校验结论同步注册清单；表格式按已核准版本决定可写字段
4. 并发登记：另一候选先注册时的冲突重核验与实际核准依据
5. 已经开始写入的任务按原版本继续
"""
from __future__ import annotations

import threading

from app.services import service_for
from app.store import store

# 基准 v1：金额列 decimal(12,2) 必填
V1_FIELDS = [
    {"name": "order_id", "type": "string", "nullable": False},
    {"name": "amount", "type": "decimal(12,2)", "nullable": False},
    {"name": "ts", "type": "string", "nullable": True},
]


def _fields_with_amount(amount_type: str, extra: list[dict] | None = None) -> list[dict]:
    fields = [
        {"name": "order_id", "type": "string", "nullable": False},
        {"name": "amount", "type": amount_type, "nullable": False},
        {"name": "ts", "type": "string", "nullable": True},
    ]
    return fields + list(extra or [])


def _new_schema_entry(topic: str, version: str) -> dict:
    sg = service_for("schema_govern")
    entry, missing = sg.create_entry({
        "主题名称": topic,
        "Schema定义": "{}",
        "版本号": version,
    })
    assert not missing
    return entry


def _new_table_entry(table: str, config_id: str) -> dict:
    tf = service_for("table_format")
    entry, missing = tf.create_entry({
        "配置编号": config_id,
        "目标表": table,
        "表格式类型": "iceberg",
        "写入模式": "append",
    })
    assert not missing
    entry["status"] = "已生效"
    entry["pending"] = False
    return entry


def _validate(topic: str, version: str, fields: list[dict]) -> dict:
    sg = service_for("schema_govern")
    entry = _new_schema_entry(topic, version)
    return sg.validate_compatibility(entry["id"], {"version": version, "fields": fields})


# ============================================================
# 1. decimal 总位数与小数位数
# ============================================================
class TestDecimalPrecisionRules:
    def test_decimal_narrow_total_digits_rejected(self):
        """decimal(12,2) → decimal(10,2)：总位数缩窄，旧记录可能溢出，判不兼容"""
        topic = "compat_decimal_narrow"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        result = _validate(topic, "v2", _fields_with_amount("decimal(10,2)"))
        assert result["ok"] is False
        assert "总位数" in result["reason"]
        assert "decimal(12,2)" in result["reason"]
        assert "decimal(10,2)" in result["reason"]

        report = result["report"]
        assert report["compatible"] is False
        amount_detail = next(d for d in report["details"] if d["field"] == "amount")
        assert amount_detail["compatible"] is False
        assert amount_detail["change"] == "type"

        # 校验结论已同步到注册清单（待启用候选）
        candidate = store.find_schema_candidate(topic, "v2")
        assert candidate is not None
        assert candidate["base_version"] == "v1"
        assert candidate["compatibility"]["compatible"] is False

    def test_decimal_widen_total_digits_accepted(self):
        """decimal(12,2) → decimal(14,2)：总位数放宽，旧记录可读，判兼容"""
        topic = "compat_decimal_widen"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        result = _validate(topic, "v2", _fields_with_amount("decimal(14,2)"))
        assert result["ok"] is True
        assert result["base_version"] == "v1"

        report = result["report"]
        assert report["compatible"] is True
        amount_detail = next(d for d in report["details"] if d["field"] == "amount")
        assert amount_detail["compatible"] is True
        assert "精度放宽" in amount_detail["reason"]

    def test_decimal_integer_digits_narrow_rejected(self):
        """decimal(12,2) → decimal(12,3)：总位数不变但整数位缩窄，判不兼容"""
        topic = "compat_decimal_int_digits"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        result = _validate(topic, "v2", _fields_with_amount("decimal(12,3)"))
        assert result["ok"] is False
        assert "整数位" in result["reason"]

    def test_decimal_scale_narrow_rejected(self):
        """decimal(12,2) → decimal(12,1)：小数位缩窄，旧记录小数无法精确读取"""
        topic = "compat_decimal_scale"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        result = _validate(topic, "v2", _fields_with_amount("decimal(12,1)"))
        assert result["ok"] is False
        assert "小数位" in result["reason"]


# ============================================================
# 2. 可空性、默认值、字段重排
# ============================================================
class TestFieldLevelRules:
    def test_required_field_with_default_accepted(self):
        """新增有默认值的必填字段：旧记录按默认值读取，判兼容"""
        topic = "compat_add_default"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = _fields_with_amount("decimal(12,2)", extra=[
            {"name": "currency", "type": "string", "nullable": False, "default": "CNY"},
        ])
        result = _validate(topic, "v2", fields)
        assert result["ok"] is True

        detail = next(d for d in result["report"]["details"] if d["field"] == "currency")
        assert detail["compatible"] is True
        assert "默认值" in detail["reason"]

    def test_required_field_without_default_rejected(self):
        """新增无默认值的必填字段：旧记录无法补齐，判不兼容"""
        topic = "compat_add_no_default"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = _fields_with_amount("decimal(12,2)", extra=[
            {"name": "settle_date", "type": "string", "nullable": False},
        ])
        result = _validate(topic, "v2", fields)
        assert result["ok"] is False
        assert "无默认值" in result["reason"]
        assert "settle_date" in result["reason"]

    def test_nullable_field_accepted(self):
        """新增可空字段：旧记录读取为空值，判兼容"""
        topic = "compat_add_nullable"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = _fields_with_amount("decimal(12,2)", extra=[
            {"name": "remark", "type": "string", "nullable": True},
        ])
        result = _validate(topic, "v2", fields)
        assert result["ok"] is True

    def test_nullable_to_required_rejected(self):
        """可空 → 必填：旧记录可能含空值，判不兼容"""
        topic = "compat_nullable_required"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = [
            {"name": "order_id", "type": "string", "nullable": False},
            {"name": "amount", "type": "decimal(12,2)", "nullable": False},
            {"name": "ts", "type": "string", "nullable": False},  # 可空改为必填
        ]
        result = _validate(topic, "v2", fields)
        assert result["ok"] is False
        assert "可空" in result["reason"]
        assert "必填" in result["reason"]

    def test_field_reorder_compatible(self):
        """字段重排：按名称读取不受影响，判兼容并记录提示"""
        topic = "compat_reorder"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = [
            {"name": "ts", "type": "string", "nullable": True},
            {"name": "amount", "type": "decimal(12,2)", "nullable": False},
            {"name": "order_id", "type": "string", "nullable": False},
        ]
        result = _validate(topic, "v2", fields)
        assert result["ok"] is True

        reorder_details = [d for d in result["report"]["details"] if d["change"] == "reorder"]
        assert len(reorder_details) >= 1
        assert all(d["compatible"] for d in reorder_details)
        assert "重排" in reorder_details[0]["reason"]

    def test_removed_field_rejected(self):
        """删除已有字段：旧记录无法被新定义完整读取，判不兼容"""
        topic = "compat_removed"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        fields = [
            {"name": "order_id", "type": "string", "nullable": False},
            {"name": "amount", "type": "decimal(12,2)", "nullable": False},
        ]
        result = _validate(topic, "v2", fields)
        assert result["ok"] is False
        assert "ts" in result["reason"]


# ============================================================
# 3. 表格式按已核准版本决定可写字段
# ============================================================
class TestTableFormatApprovedConstraints:
    def test_table_format_uses_approved_version_writable_fields(self):
        """表格式只采用已核准版本的字段约束；候选版本确认前不生效"""
        sg = service_for("schema_govern")
        tf = service_for("table_format")
        table = "compat_tf_approved"
        store.register_schema(table, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")
        tf_entry = _new_table_entry(table, "TABL-COMPAT-APPROVED")

        # 候选 v2（新增可空字段）校验通过但未确认：表格式仍按 v1
        sg_entry = _new_schema_entry(table, "v2")
        result = sg.validate_compatibility(sg_entry["id"], {
            "version": "v2",
            "fields": _fields_with_amount("decimal(12,2)", extra=[
                {"name": "remark", "type": "string", "nullable": True},
            ]),
        })
        assert result["ok"] is True

        strategy = tf.resolve_write_strategy(tf_entry["id"])
        assert strategy["ok"] is True
        assert strategy["schema_version"] == "v1"
        assert strategy["writable_fields"] == ["amount", "order_id", "ts"]

        # 确认注册后：表格式采用已核准的 v2 约束
        confirm = sg.confirm_registration(sg_entry["id"])
        assert confirm["ok"] is True
        assert confirm["conflict"] is False

        strategy = tf.resolve_write_strategy(tf_entry["id"])
        assert strategy["schema_version"] == "v2"
        assert strategy["writable_fields"] == ["amount", "order_id", "remark", "ts"]
        assert strategy["field_constraints"]["amount"]["type"] == "decimal(12,2)"

    def test_unverified_candidate_not_published(self):
        """并发登记时只发布经过核验的字段约束和注册版本组合"""
        sg = service_for("schema_govern")
        topic = "compat_unverified"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        # 候选 v2 校验失败（新增无默认值必填字段）
        sg_entry = _new_schema_entry(topic, "v2")
        result = sg.validate_compatibility(sg_entry["id"], {
            "version": "v2",
            "fields": _fields_with_amount("decimal(12,2)", extra=[
                {"name": "settle_date", "type": "string", "nullable": False},
            ]),
        })
        assert result["ok"] is False

        # 确认注册被拒绝，注册清单中已发布版本不变
        confirm = sg.confirm_registration(sg_entry["id"])
        assert confirm["ok"] is False
        assert "核验未通过" in confirm["reason"]
        assert [v["version"] for v in store.published_schema_versions(topic)] == ["v1"]
        candidate = store.find_schema_candidate(topic, "v2")
        assert candidate["status"] == "rejected"


# ============================================================
# 4. 并发登记冲突：另一候选先注册
# ============================================================
class TestConcurrentRegistration:
    def test_conflict_revalidated_against_actual_base(self):
        """另一候选先注册：按实际最新版本重新核验，待启用定义记录实际核准依据"""
        sg = service_for("schema_govern")
        topic = "compat_conflict_ok"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        # 候选A：v2，amount 放宽到 decimal(14,2)，基于 v1 校验通过
        entry_a = _new_schema_entry(topic, "v2")
        result_a = sg.validate_compatibility(entry_a["id"], {
            "version": "v2", "fields": _fields_with_amount("decimal(14,2)"),
        })
        assert result_a["ok"] is True
        assert result_a["base_version"] == "v1"

        # 候选B：v3，amount 放宽到 decimal(13,2)，先确认注册
        entry_b = _new_schema_entry(topic, "v3")
        result_b = sg.validate_compatibility(entry_b["id"], {
            "version": "v3", "fields": _fields_with_amount("decimal(13,2)"),
        })
        assert result_b["ok"] is True
        confirm_b = sg.confirm_registration(entry_b["id"])
        assert confirm_b["ok"] is True
        assert confirm_b["conflict"] is False

        # A 确认时基线已是 v3：冲突 → 按 v3 重新核验，decimal(14,2) 仍兼容
        confirm_a = sg.confirm_registration(entry_a["id"])
        assert confirm_a["ok"] is True
        assert confirm_a["conflict"] is True
        basis = confirm_a["approval_basis"]
        assert basis["original_base_version"] == "v1"
        assert basis["actual_base_version"] == "v3"
        assert basis["revalidated"] is True
        assert basis["original_verdict"] is True

        # 注册清单中的定义已记录实际核准依据
        record = store.find_schema_version(topic, "v2")
        assert record["status"] == "published"
        assert record["approval_basis"]["actual_base_version"] == "v3"

    def test_conflict_original_approval_superseded(self):
        """另一候选先注册导致原批准结论失效：记录原结论，表格式采用实际核准约束"""
        sg = service_for("schema_govern")
        tf = service_for("table_format")
        topic = "compat_conflict_reject"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        # 候选A：v2，amount decimal(13,2)，基于 v1 的原批准结论为兼容
        entry_a = _new_schema_entry(topic, "v2")
        result_a = sg.validate_compatibility(entry_a["id"], {
            "version": "v2", "fields": _fields_with_amount("decimal(13,2)"),
        })
        assert result_a["ok"] is True

        # 候选B：v3，amount decimal(14,2)，先注册
        entry_b = _new_schema_entry(topic, "v3")
        result_b = sg.validate_compatibility(entry_b["id"], {
            "version": "v3", "fields": _fields_with_amount("decimal(14,2)"),
        })
        assert result_b["ok"] is True
        assert sg.confirm_registration(entry_b["id"])["ok"] is True

        # A 确认：按 v3(decimal(14,2)) 重新核验，decimal(13,2) 总位数缩窄 → 拒绝
        confirm_a = sg.confirm_registration(entry_a["id"])
        assert confirm_a["ok"] is False
        assert confirm_a["conflict"] is True
        basis = confirm_a["approval_basis"]
        assert basis["original_verdict"] is True           # 原批准结论保留
        assert basis["original_base_version"] == "v1"
        assert basis["actual_base_version"] == "v3"
        assert basis["report"]["compatible"] is False      # 实际核准依据判定不兼容
        assert "总位数" in confirm_a["reason"]

        record = store.find_schema_version(topic, "v2")
        assert record["status"] == "rejected"
        assert record["approval_basis"]["original_verdict"] is True

        # 表格式实际采用已核准的 v3 约束，而非被推翻的 v2
        tf_entry = _new_table_entry(topic, "TABL-COMPAT-CONFLICT")
        strategy = tf.resolve_write_strategy(tf_entry["id"])
        assert strategy["ok"] is True
        assert strategy["schema_version"] == "v3"
        assert strategy["field_constraints"]["amount"]["type"] == "decimal(14,2)"

    def test_concurrent_confirm_publishes_only_verified_combos(self):
        """并发确认：恰好一个直接发布，另一个冲突后按实际基线重核验发布"""
        sg = service_for("schema_govern")
        topic = "compat_concurrent"
        store.register_schema(topic, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")

        # 两个候选提交相同的最终字段集（均与 v1 及彼此兼容），并发确认
        final_fields = _fields_with_amount("decimal(12,2)", extra=[
            {"name": "currency", "type": "string", "nullable": True},
            {"name": "remark", "type": "string", "nullable": True},
        ])
        entry_a = _new_schema_entry(topic, "v2")
        entry_b = _new_schema_entry(topic, "v3")
        assert sg.validate_compatibility(
            entry_a["id"], {"version": "v2", "fields": final_fields})["ok"] is True
        assert sg.validate_compatibility(
            entry_b["id"], {"version": "v3", "fields": final_fields})["ok"] is True

        results: dict[str, dict] = {}
        threads = [
            threading.Thread(target=lambda: results.__setitem__(
                "a", sg.confirm_registration(entry_a["id"]))),
            threading.Thread(target=lambda: results.__setitem__(
                "b", sg.confirm_registration(entry_b["id"]))),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results["a"]["ok"] is True
        assert results["b"]["ok"] is True
        # 恰好一个无冲突直接发布，另一个按实际基线重核验后发布
        assert sorted([results["a"]["conflict"], results["b"]["conflict"]]) == [False, True]
        published = [v["version"] for v in store.published_schema_versions(topic)]
        assert published == ["v1", "v2", "v3"]
        # 经治理路径发布的组合均经过核验；所有已发布版本都带字段约束
        for version in store.published_schema_versions(topic):
            assert version["constraints"]
            if version["version"] != "v1":  # v1 为低层打底数据，未经治理路径
                assert version["compatibility"]["compatible"] is True


# ============================================================
# 5. 已经开始写入的任务按原版本继续
# ============================================================
class TestActiveWriteTaskPinning:
    def test_active_write_task_continues_original_version(self):
        """写入任务进行中发布新版本：任务按原版本继续，结束后按新版本"""
        sg = service_for("schema_govern")
        tf = service_for("table_format")
        table = "compat_write_task"
        store.register_schema(table, {"fields": _fields_with_amount("decimal(12,2)")}, "v1")
        tf_entry = _new_table_entry(table, "TABL-COMPAT-WT")

        # 开始写入任务：固定 v1 的字段约束
        begin = tf.begin_write_task(tf_entry["id"])
        assert begin["ok"] is True
        assert begin["schema_version"] == "v1"
        assert begin["writable_fields"] == ["amount", "order_id", "ts"]

        # 期间 v2 发布（新增可空字段）
        sg_entry = _new_schema_entry(table, "v2")
        result = sg.validate_compatibility(sg_entry["id"], {
            "version": "v2",
            "fields": _fields_with_amount("decimal(12,2)", extra=[
                {"name": "remark", "type": "string", "nullable": True},
            ]),
        })
        assert result["ok"] is True
        assert sg.confirm_registration(sg_entry["id"])["ok"] is True

        # 进行中的写入任务按原版本继续
        strategy = tf.resolve_write_strategy(tf_entry["id"])
        assert strategy["schema_version"] == "v1"
        assert strategy["writable_fields"] == ["amount", "order_id", "ts"]
        assert strategy["write_task_continues_original"] is True

        # 重复开始返回原任务，不会切换版本
        begin_again = tf.begin_write_task(tf_entry["id"])
        assert begin_again["schema_version"] == "v1"

        # 任务结束后按最新核准版本写入
        finish = tf.finish_write_task(tf_entry["id"])
        assert finish["ok"] is True
        assert finish["schema_version"] == "v1"
        strategy = tf.resolve_write_strategy(tf_entry["id"])
        assert strategy["schema_version"] == "v2"
        assert strategy["writable_fields"] == ["amount", "order_id", "remark", "ts"]
        assert strategy["write_task_continues_original"] is False

    def test_begin_write_task_requires_approved_schema(self):
        """无已核准版本的表无法开始写入任务"""
        tf = service_for("table_format")
        tf_entry = _new_table_entry("compat_no_schema_table", "TABL-COMPAT-NOSCHEMA")
        result = tf.begin_write_task(tf_entry["id"])
        assert result["ok"] is False
        assert "无已核准" in result["reason"]
