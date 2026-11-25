"""逐字段向后兼容规则：判定标准以旧记录能否被新定义读取为准。

规则维度：
- 总位数与小数位数：decimal(p,s) 总位数/小数位/整数位缩窄判为不兼容，放宽判为兼容
- 可空性：可空 → 必填 判为不兼容（旧记录可能含空值，新定义读不出来）
- 默认值：新增必填字段无默认值判为不兼容；含默认值时旧记录按默认值读取，判为兼容
- 字段重排：按字段名读取，位置变化判为兼容并记录提示
"""
from __future__ import annotations

import re
import time
from typing import Any

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)

# 已知的类型放宽关系：旧类型 → 新定义可直接读取旧记录的类型集合
_TYPE_WIDENING: dict[str, set[str]] = {
    "int": {"bigint", "long", "float", "double", "string"},
    "bigint": {"double", "string"},
    "long": {"double", "string"},
    "float": {"double", "string"},
    "double": {"string"},
    "date": {"timestamp", "string"},
}


def normalize_type(type_str: Any) -> str | None:
    if type_str is None:
        return None
    text = str(type_str).strip().lower()
    return text or None


def parse_decimal(type_str: Any) -> tuple[int, int] | None:
    """解析 decimal(p,s)，返回 (总位数, 小数位数)；非 decimal 返回 None。"""
    text = normalize_type(type_str)
    if text is None:
        return None
    match = _DECIMAL_RE.match(text)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def normalize_fields(schema_def: dict[str, Any] | None) -> list[dict[str, Any]]:
    """把字段定义归一化为 [{name, type, nullable, has_default, default, position}]。

    兼容两种写法：
    - 字符串简写: "amount"（视为可空、无默认值、未声明类型）
    - 字典完整定义: {"name": "amount", "type": "decimal(12,2)", "nullable": False, "default": "0.00"}
    """
    if not isinstance(schema_def, dict):
        return []
    raw_fields = schema_def.get("fields", [])
    if not isinstance(raw_fields, list):
        return []
    normalized: list[dict[str, Any]] = []
    for idx, field in enumerate(raw_fields):
        if isinstance(field, dict):
            default = field.get("default")
            normalized.append({
                "name": str(field.get("name", f"field_{idx}")),
                "type": normalize_type(field.get("type")),
                "nullable": bool(field.get("nullable", True)),
                "has_default": default is not None,
                "default": default,
                "position": idx,
            })
        else:
            normalized.append({
                "name": str(field),
                "type": None,
                "nullable": True,
                "has_default": False,
                "default": None,
                "position": idx,
            })
    return normalized


def extract_constraints(schema_def: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """提取字段约束：随注册版本一起发布、供表格式决定可写字段的核验结果。"""
    return {
        field["name"]: {
            "type": field["type"],
            "nullable": field["nullable"],
            "has_default": field["has_default"],
            "default": field["default"],
        }
        for field in normalize_fields(schema_def)
    }


def check_type_compatible(old_type: Any, new_type: Any) -> tuple[bool, str]:
    """类型层面判断旧记录能否被新定义读取，返回 (是否兼容, 原因)。"""
    old = normalize_type(old_type)
    new = normalize_type(new_type)
    if old is None or new is None:
        return True, "未声明类型，按字段名兼容读取"
    if old == new:
        return True, f"类型一致 {old}"

    old_dec = parse_decimal(old)
    new_dec = parse_decimal(new)
    if old_dec and new_dec:
        old_p, old_s = old_dec
        new_p, new_s = new_dec
        if new_s < old_s:
            return False, (
                f"decimal 小数位数缩窄: decimal({old_p},{old_s}) → decimal({new_p},{new_s})，"
                f"旧记录小数部分无法精确读取"
            )
        if new_p < old_p:
            return False, (
                f"decimal 总位数缩窄: decimal({old_p},{old_s}) → decimal({new_p},{new_s})，"
                f"旧记录数值可能超出新定义范围"
            )
        if (new_p - new_s) < (old_p - old_s):
            return False, (
                f"decimal 整数位缩窄: decimal({old_p},{old_s}) → decimal({new_p},{new_s})，"
                f"旧记录整数部分可能溢出"
            )
        return True, f"decimal 精度放宽: decimal({old_p},{old_s}) → decimal({new_p},{new_s})"

    if new in _TYPE_WIDENING.get(old, set()):
        return True, f"类型放宽 {old} → {new}"
    return False, f"类型变更 {old} → {new}，旧记录无法按新类型读取"


def check_backward_compatibility(
    old_def: dict[str, Any] | None,
    new_def: dict[str, Any] | None,
) -> dict[str, Any]:
    """逐字段向后兼容校验：旧记录能否被新定义读取。

    返回 {"compatible", "reason", "details", ...}，details 按字段给出判定结果。
    """
    old_fields = normalize_fields(old_def)
    new_fields = normalize_fields(new_def)
    new_by_name = {f["name"]: f for f in new_fields}
    old_names = {f["name"] for f in old_fields}

    details: list[dict[str, Any]] = []
    compatible = True

    for old_field in old_fields:
        name = old_field["name"]
        new_field = new_by_name.get(name)
        if new_field is None:
            compatible = False
            details.append({
                "field": name, "change": "removed", "compatible": False,
                "reason": f"删除已有字段 '{name}'，旧记录无法被新定义完整读取",
            })
            continue

        type_ok, type_reason = check_type_compatible(old_field["type"], new_field["type"])
        type_changed = normalize_type(old_field["type"]) != normalize_type(new_field["type"])
        if not type_ok:
            compatible = False
            details.append({
                "field": name, "change": "type", "compatible": False,
                "reason": f"字段 '{name}' {type_reason}",
            })
        elif type_changed:
            details.append({
                "field": name, "change": "type", "compatible": True,
                "reason": f"字段 '{name}' {type_reason}",
            })

        nullable_issue = old_field["nullable"] and not new_field["nullable"]
        if nullable_issue:
            compatible = False
            details.append({
                "field": name, "change": "nullability", "compatible": False,
                "reason": f"字段 '{name}' 由可空变为必填，旧记录中的空值无法被新定义读取",
            })

        reordered = old_field["position"] != new_field["position"]
        if reordered:
            details.append({
                "field": name, "change": "reorder", "compatible": True,
                "reason": (
                    f"字段 '{name}' 位置重排 ({old_field['position']}→{new_field['position']})，"
                    f"按名称读取不受影响"
                ),
            })

        if type_ok and not type_changed and not nullable_issue and not reordered:
            details.append({
                "field": name, "change": "unchanged", "compatible": True,
                "reason": f"字段 '{name}' 定义一致",
            })

    for new_field in new_fields:
        name = new_field["name"]
        if name in old_names:
            continue
        if new_field["nullable"]:
            details.append({
                "field": name, "change": "added", "compatible": True,
                "reason": f"新增可空字段 '{name}'，旧记录读取为空值",
            })
        elif new_field["has_default"]:
            details.append({
                "field": name, "change": "added", "compatible": True,
                "reason": f"新增必填字段 '{name}' 含默认值 {new_field['default']!r}，旧记录按默认值读取",
            })
        else:
            compatible = False
            details.append({
                "field": name, "change": "added", "compatible": False,
                "reason": f"新增必填字段 '{name}' 无默认值，旧记录无法补齐该字段",
            })

    failures = [d["reason"] for d in details if not d["compatible"]]
    return {
        "compatible": compatible,
        "reason": "；".join(failures) if failures else "向后兼容：旧记录可被新定义读取",
        "details": details,
        "old_field_count": len(old_fields),
        "new_field_count": len(new_fields),
        "checked_at": time.time(),
    }
