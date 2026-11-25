"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from app.schema_compat import check_backward_compatibility, extract_constraints
from app.seed import SEED_ROWS


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # Schema 注册/发布锁：并发登记时保证只发布经过核验的组合
        self._schema_lock = threading.Lock()
        # 进行中的写入任务（表格式按任务启动时核准的 Schema 版本继续写入）
        self._active_write_tasks: dict[str, dict[str, Any]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str,
                        *, status: str = "published",
                        compatibility: dict[str, Any] | None = None,
                        base_version: str | None = None) -> dict[str, Any]:
        """低层登记一个 Schema 版本（默认已发布，供既有调用方与测试打底数据使用）。

        治理路径应使用 add_schema_candidate + publish_schema，
        保证只发布经过核验的字段约束和注册版本组合。
        """
        entry = {
            "topic": topic,
            "definition": schema_def,
            "version": version,
            "ts": time.time(),
            "status": status,
            "base_version": base_version,
            "compatibility": compatibility,
            "constraints": extract_constraints(schema_def),
            "approval_basis": None,
        }
        with self._schema_lock:
            self._schema_registry.setdefault(topic, []).append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def published_schema_versions(self, topic: str) -> list[dict[str, Any]]:
        """仅已核准（已发布）的版本：表格式只消费这个清单。"""
        return [v for v in self.schema_versions(topic) if v.get("status") == "published"]

    def latest_published_schema(self, topic: str) -> dict[str, Any] | None:
        published = self.published_schema_versions(topic)
        return published[-1] if published else None

    def find_schema_version(self, topic: str, version: str) -> dict[str, Any] | None:
        for v in self.schema_versions(topic):
            if v.get("version") == version:
                return v
        return None

    def find_schema_candidate(self, topic: str, version: str) -> dict[str, Any] | None:
        """查找待启用/已拒绝的候选版本（已发布版本不可再作为候选）。"""
        for v in self.schema_versions(topic):
            if v.get("version") == version and v.get("status") in ("candidate", "rejected"):
                return v
        return None

    def add_schema_candidate(self, topic: str, schema_def: dict[str, Any], version: str,
                             *, base_version: str | None,
                             compatibility: dict[str, Any]) -> dict[str, Any]:
        """校验结论同步到注册清单：登记或刷新待启用候选版本。"""
        with self._schema_lock:
            existing = self.find_schema_candidate(topic, version)
            if existing is not None:
                existing.update({
                    "definition": schema_def,
                    "base_version": base_version,
                    "compatibility": compatibility,
                    "constraints": extract_constraints(schema_def),
                    "status": "candidate",
                    "ts": time.time(),
                })
                return existing
            entry = {
                "topic": topic,
                "definition": schema_def,
                "version": version,
                "ts": time.time(),
                "status": "candidate",
                "base_version": base_version,
                "compatibility": compatibility,
                "constraints": extract_constraints(schema_def),
                "approval_basis": None,
            }
            self._schema_registry.setdefault(topic, []).append(entry)
            return entry

    def publish_schema(self, topic: str, version: str,
                       *, expected_base: str | None = None) -> dict[str, Any]:
        """确认发布候选版本（并发安全）。

        - 只发布经过核验（校验结论 compatible）的字段约束与注册版本组合
        - expected_base 与当前最新已发布版本不一致时判定为并发冲突：
          按实际最新版本重新核验，并在待启用定义上记录实际核准依据
        """
        with self._schema_lock:
            candidate = self.find_schema_candidate(topic, version)
            if candidate is None:
                return {"ok": False, "conflict": False,
                        "reason": f"主题 {topic} 版本 {version} 无待启用候选"}
            latest = self.latest_published_schema(topic)
            actual_base = latest.get("version") if latest else None
            conflict = actual_base != expected_base
            report = candidate.get("compatibility") or {
                "compatible": False, "reason": "未经过兼容性核验", "details": [],
            }
            original_verdict = report.get("compatible")
            if conflict:
                # 另一候选已先注册：按实际最新已发布版本重新核验
                base_def = latest.get("definition") if latest else {"fields": []}
                report = check_backward_compatibility(base_def, candidate.get("definition"))
            candidate["approval_basis"] = {
                "original_base_version": expected_base,
                "actual_base_version": actual_base,
                "original_verdict": original_verdict,
                "revalidated": conflict,
                "report": report,
                "ts": time.time(),
            }
            if not report.get("compatible"):
                candidate["status"] = "rejected"
                return {"ok": False, "conflict": conflict,
                        "reason": f"版本 {version} 核验未通过: {report.get('reason')}",
                        "report": report}
            candidate["status"] = "published"
            candidate["compatibility"] = report
            candidate["published_ts"] = time.time()
            return {"ok": True, "conflict": conflict, "topic": topic, "version": version,
                    "base_version": actual_base, "report": report}

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        base = self.latest_published_schema(topic)
        if base is None:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        report = check_backward_compatibility(base.get("definition"), {"fields": list(new_fields)})
        return {"compatible": report["compatible"], "reason": report["reason"]}

    # ---------- 写入任务（表格式按核准版本固定写入约束） ----------
    def start_write_task(self, table: str, task_id: str | None = None) -> dict[str, Any] | None:
        """开始写入任务：固定当前已核准的字段约束与注册版本组合。

        已有进行中的任务时直接返回原任务：已经开始写入的任务按原版本继续。
        """
        with self._schema_lock:
            existing = self._active_write_tasks.get(table)
            if existing is not None:
                return existing
            latest = self.latest_published_schema(table)
            if latest is None:
                return None
            constraints = {
                name: dict(constraint)
                for name, constraint in (latest.get("constraints") or {}).items()
            }
            task = {
                "table": table,
                "task_id": task_id or f"WT-{table}-{int(time.time() * 1000)}",
                "schema_version": latest.get("version"),
                "constraints": constraints,
                "started_ts": time.time(),
            }
            self._active_write_tasks[table] = task
            return task

    def active_write_task(self, table: str) -> dict[str, Any] | None:
        return self._active_write_tasks.get(table)

    def finish_write_task(self, table: str) -> dict[str, Any] | None:
        with self._schema_lock:
            return self._active_write_tasks.pop(table, None)

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]


store = Store()