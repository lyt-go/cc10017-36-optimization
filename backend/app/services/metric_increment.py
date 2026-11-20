"""订单吞吐指标增量计算服务。

设计目标（对应需求）：
1. 旧实现每次刷新都全量重扫 topic_registry / flink_job，刷新越频繁耗时越长；
   这里把"源版本流 → 增量结果表 → 已消费版本游标"拆开，每次刷新只处理
   「上次已消费版本之后」的新版本，计算量与新增窗口数成正比，与历史总量无关。
2. 指标增量结果表保留已消费版本与新增聚合值：
   - metric_increment_results：逐窗口的增量聚合行，记录写入时所属表版本；
   - metric_increment_cursors：每个指标的已消费版本、结果表当前版本。
3. 写入中断恢复：中断只写入部分批次行，游标不前进；恢复时按连续区间
   补齐未消费版本，结果表按 (metric_id, source_version, window_start)
   去重，同一窗口绝不重复累计。
4. 冲突取舍（已消费版本与新增短窗口在同一刷新周期相遇）：
   固定采用「先并入新增窗口（去重写入）→ 再扩展已消费版本」的顺序，
   保证连续区间唯一、结果表版本可复现。
5. 跨模块：把结果表当前版本交付给数据生命周期（ttl_delivery_log），
   生命周期模块据保留期判断主题是否仍可消费。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from app.store import store

METRIC_RESULTS_TABLE = "metric_increment_results"
CURSOR_TABLE = "metric_increment_cursors"
SOURCE_VERSION_TABLE = "metric_source_versions"
DELIVERY_TABLE = "ttl_delivery_log"

# 结果表首次创建时的初始版本；每次成功提交（游标推进）后版本 +1。
INITIAL_TABLE_VERSION = 1


class IncrementWriteInterrupted(Exception):
    """模拟写入中断：已写入 fail_after 个新窗口后抛出，游标保持不动。"""

    def __init__(self, partial: dict[str, Any]) -> None:
        super().__init__("指标增量结果表写入中断")
        self.partial = partial


def _parse_duration_days(text: Any) -> int | None:
    """把 '7d' / '30d' 这类保留时长解析成天数，无法解析返回 None。"""
    if text is None:
        return None
    match = re.match(r"^\s*(\d+)\s*([dhm])?\s*$", str(text).strip().casefold())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2) or "d"
    if unit == "h":
        return max(1, value // 24)
    if unit == "m":
        return 1
    return value


def _window_minutes(window_text: str) -> int:
    """聚合窗口（'1min'/'5min'/'1h'）换算成分钟，用于生成窗口起点。"""
    text = (window_text or "1min").strip().casefold()
    match = re.match(r"^(\d+)\s*(min|m|h|d)?$", text)
    if not match:
        return 1
    value = int(match.group(1))
    unit = match.group(2) or "min"
    if unit == "h":
        return value * 60
    if unit == "d":
        return value * 60 * 24
    return value  # min / m 都按分钟


class MetricIncrementService:
    """订单吞吐指标的增量计算、中断恢复与跨模块交付。"""

    # ───────────────────────────────────
    # 源版本流：替代"每次全量重扫"
    # ───────────────────────────────────

    def ingest_source_versions(self, metric_id: int, count: int = 1) -> dict[str, Any]:
        """模拟短窗口源版本到达：每个版本对应一个新到达的聚合窗口。

        源版本由主题（topic_registry）与流任务结果（flink_job）派生，
        仅在接入新版本时读取一次源表，历史窗口沉淀在源版本流里，
        后续增量刷新不再重扫全部主题和流任务。
        """
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        store.rows(SOURCE_VERSION_TABLE)
        existed = [r for r in store.rows(SOURCE_VERSION_TABLE) if r.get("metric_id") == metric_id]
        next_version = max((int(r["source_version"]) for r in existed), default=0) + 1

        # 只扫描当前在线的主题与运行中的流任务（有界扫描，仅为新版本取数）
        online_topics = [r for r in store.rows("topic_registry") if r.get("status") == "已上线"]
        running_jobs = [r for r in store.rows("flink_job") if r.get("status") == "运行中"]
        window_minutes = _window_minutes(str(metric.get("聚合窗口", "1min")))
        base_time = datetime(2026, 11, 16, 10, 0, 0)

        created: list[dict[str, Any]] = []
        for offset in range(count):
            version = next_version + offset
            window_start = base_time + timedelta(minutes=window_minutes * (version - 1))
            row = {
                "source_version": version,
                "metric_id": metric_id,
                "topic_code": "TOPI-0001",  # 订单吞吐指标的来源主题
                "window_start": window_start.isoformat(),
                "window_end": (window_start + timedelta(minutes=window_minutes)).isoformat(),
                # 确定性的原始观测值：同一窗口重复计算结果一致，便于校验无重复累计
                "observed_records": 100 + version * 7,
                "source_topics": len(online_topics),
                "source_flink_jobs": len(running_jobs),
                "arrived_at": datetime.now().isoformat(),
            }
            store.rows(SOURCE_VERSION_TABLE).append(row)
            created.append(row)

        return {"ok": True, "metric_id": metric_id, "ingested": len(created), "versions": created}

    def list_source_versions(self, metric_id: int) -> list[dict[str, Any]]:
        return [
            r for r in store.rows(SOURCE_VERSION_TABLE)
            if r.get("metric_id") == metric_id
        ]

    # ───────────────────────────────────
    # 游标与结果表
    # ───────────────────────────────────

    def _get_cursor(self, metric_id: int) -> dict[str, Any]:
        rows = store.rows(CURSOR_TABLE)
        cursor = next((r for r in rows if r.get("metric_id") == metric_id), None)
        if cursor is None:
            cursor = {
                "metric_id": metric_id,
                "consumed_version": 0,          # 已消费版本（0 表示尚未消费任何版本）
                "table_version": INITIAL_TABLE_VERSION - 1,
                "last_committed_at": None,
                "last_batch_id": None,
                "updated_at": None,
            }
            rows.append(cursor)
        return cursor

    def get_cursor(self, metric_id: int) -> dict[str, Any]:
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}
        cursor = self._get_cursor(metric_id)
        return {"ok": True, "cursor": dict(cursor)}

    def list_results(
        self,
        metric_id: int | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        only_new: bool = False,
    ) -> list[dict[str, Any]]:
        """查询增量结果表，支持按时间窗口筛选、只看新增未消费聚合值。"""
        rows = store.rows(METRIC_RESULTS_TABLE)
        if metric_id is not None:
            rows = [r for r in rows if r.get("metric_id") == metric_id]
        if window_start:
            rows = [r for r in rows if r.get("window_start", "") >= window_start]
        if window_end:
            rows = [r for r in rows if r.get("window_start", "") <= window_end]
        if only_new:
            rows = [r for r in rows if r.get("row_state") == "新增"]
        return [dict(r) for r in rows]

    # ───────────────────────────────────
    # 增量刷新（含中断模拟与恢复）
    # ───────────────────────────────────

    def _aggregate(self, metric: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        """对单个源版本窗口做聚合，取值确定、可复算。"""
        agg_func = metric.get("聚合函数", "count")
        observed = int(source["observed_records"])
        if agg_func == "count":
            value = observed
        elif agg_func == "sum":
            value = observed * 100
        elif agg_func == "avg":
            value = round(observed / max(1, int(source["source_flink_jobs"])), 2)
        else:
            value = observed
        return {
            "metric_id": metric["id"],
            "metric_code": metric.get("指标编号"),
            "metric_name": metric.get("指标名称"),
            "output_table": metric.get("输出表名"),
            "agg_window": metric.get("聚合窗口"),
            "agg_func": agg_func,
            "source_version": int(source["source_version"]),
            "topic_code": source.get("topic_code"),
            "window_start": source["window_start"],
            "window_end": source["window_end"],
            "agg_value": value,
            "source_topics": source["source_topics"],
            "source_flink_jobs": source["source_flink_jobs"],
        }

    def _existing_keys(self, metric_id: int) -> set[tuple[int, str]]:
        return {
            (int(r["source_version"]), str(r["window_start"]))
            for r in store.rows(METRIC_RESULTS_TABLE)
            if r.get("metric_id") == metric_id
        }

    def refresh_increment(
        self,
        metric_id: int,
        *,
        fail_after: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """从上次已消费版本之后连续推进，增量写入新窗口聚合值。

        冲突取舍固定为：先并入新增窗口（按业务键去重写入），
        全部成功后再扩展已消费版本（游标连续推进）。

        fail_after：模拟写入中断——写入该数量的新窗口后抛出
        IncrementWriteInterrupted，此时游标保持旧值，已写行保留为
        row_state='新增'，等待 recover 补齐。
        """
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        cursor = self._get_cursor(metric_id)
        consumed_before = int(cursor["consumed_version"])
        table_version_before = int(cursor["table_version"])
        batch = batch_id or f"BATCH-{metric_id}-{consumed_before}-{datetime.now().strftime('%H%M%S%f')}"

        sources = sorted(
            self.list_source_versions(metric_id),
            key=lambda r: int(r["source_version"]),
        )
        # 只取已消费版本之后的版本
        pending = [s for s in sources if int(s["source_version"]) > consumed_before]
        if not pending:
            return {
                "ok": True,
                "message": "没有已消费版本之后的新窗口，结果表保持当前版本",
                "batch_id": batch,
                "consumed_before": consumed_before,
                "consumed_after": consumed_before,
                "table_version": table_version_before,
                "written": [],
                "skipped": [],
                "gap_versions": [],
                "interrupted": False,
            }

        # 连续性校验：已消费版本之后必须是连续版本号，出现断档先告警，
        # 只推进到缺口前的最后一个连续版本，避免跳过未消费版本。
        gap_versions: list[int] = []
        contiguous: list[dict[str, Any]] = []
        expect = consumed_before + 1
        for source in pending:
            version = int(source["source_version"])
            if version != expect:
                gap_versions.extend(range(expect, version))
                break
            contiguous.append(source)
            expect += 1

        existing = self._existing_keys(metric_id)
        written: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        interrupted = False

        for source in contiguous:
            version = int(source["source_version"])
            key = (version, str(source["window_start"]))
            if key in existing:
                # 中断恢复场景：该窗口上一轮已写入，跳过且不重复累计
                skipped.append({
                    "source_version": version,
                    "window_start": source["window_start"],
                    "reason": "窗口已存在于结果表，跳过避免重复累计",
                })
                continue

            row = self._aggregate(metric, source)
            # 写入时先挂当前（尚未提交的）下一表版本；提交后即为正式版本
            row.update({
                "row_state": "新增",       # 新增聚合值；游标推进后转为"已消费"
                "result_table_version": table_version_before + 1,
                "batch_id": batch,
                "written_at": datetime.now().isoformat(),
            })
            store.rows(METRIC_RESULTS_TABLE).append(row)
            existing.add(key)
            written.append(dict(row))

            if fail_after is not None and len(written) >= fail_after:
                # 模拟写入中断：行已落表但游标不推进，连续区间停在旧位置
                interrupted = True
                raise IncrementWriteInterrupted({
                    "ok": False,
                    "interrupted": True,
                    "message": "写入中断：部分窗口已落表，已消费版本保持 %d 未推进" % consumed_before,
                    "batch_id": batch,
                    "consumed_before": consumed_before,
                    "consumed_after": consumed_before,
                    "table_version": table_version_before,
                    "written": written,
                    "skipped": skipped,
                    "gap_versions": gap_versions,
                })

        # 全部窗口写入成功后才扩展已消费版本（连续推进，杜绝重复写入）
        consumed_after = consumed_before + len(contiguous)
        for row in store.rows(METRIC_RESULTS_TABLE):
            if row.get("metric_id") == metric_id and int(row["source_version"]) <= consumed_after:
                row["row_state"] = "已消费"

        cursor["consumed_version"] = consumed_after
        cursor["table_version"] = table_version_before + 1
        cursor["last_batch_id"] = batch
        cursor["last_committed_at"] = datetime.now().isoformat()
        cursor["updated_at"] = cursor["last_committed_at"]

        return {
            "ok": True,
            "message": "增量刷新完成，已消费版本 %d → %d" % (consumed_before, consumed_after),
            "batch_id": batch,
            "consumed_before": consumed_before,
            "consumed_after": consumed_after,
            "table_version": cursor["table_version"],
            "written": written,
            "skipped": skipped,
            "gap_versions": gap_versions,
            "interrupted": False,
        }

    def recover_increment(self, metric_id: int) -> dict[str, Any]:
        """中断恢复：从上次已消费版本之后补齐未消费版本。

        结果表里可能残留中断批次的部分行，恢复时这些窗口会被识别并跳过，
        只补齐缺失窗口，然后游标一次性连续推进，同一窗口不会累计两次。
        """
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        cursor = self._get_cursor(metric_id)
        consumed_before = int(cursor["consumed_version"])
        result_rows = [r for r in store.rows(METRIC_RESULTS_TABLE) if r.get("metric_id") == metric_id]
        partial_rows = [
            r for r in result_rows
            if r.get("row_state") == "新增" and int(r["source_version"]) > consumed_before
        ]
        # 复用普通刷新路径：其中的去重逻辑会跳过 partial_rows 已覆盖的窗口
        refresh = self.refresh_increment(metric_id, batch_id=f"RECOVER-{metric_id}-{datetime.now().strftime('%H%M%S%f')}")
        refresh["recovered_partial_rows"] = [dict(r) for r in partial_rows]
        refresh["recovered"] = True
        return refresh

    # ───────────────────────────────────
    # 跨模块：实时指标 → 数据生命周期
    # ───────────────────────────────────

    def deliver_to_lifecycle(self, metric_id: int) -> dict[str, Any]:
        """把指标增量结果表（含当前版本）交付给数据生命周期模块。

        幂等：同一结果表版本重复交付只返回既有交付记录，不重复写。
        """
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        cursor = self._get_cursor(metric_id)
        table_version = int(cursor["table_version"])
        if table_version < INITIAL_TABLE_VERSION:
            return {"ok": False, "error": "结果表尚未生成任何增量版本，请先执行增量刷新"}

        store.rows(DELIVERY_TABLE)
        existing = next(
            (r for r in store.rows(DELIVERY_TABLE)
             if r.get("metric_id") == metric_id and int(r.get("result_table_version", 0)) == table_version),
            None,
        )
        if existing:
            return {
                "ok": True,
                "message": "当前版本已交付过，复用既有交付记录（幂等）",
                "delivery": dict(existing),
                "duplicated": True,
            }

        result_count = len([r for r in store.rows(METRIC_RESULTS_TABLE) if r.get("metric_id") == metric_id])
        topics = sorted({
            str(r.get("topic_code"))
            for r in store.rows(METRIC_RESULTS_TABLE)
            if r.get("metric_id") == metric_id and r.get("topic_code")
        })
        delivery = {
            "delivery_id": f"DELIV-{metric_id}-v{table_version}",
            "metric_id": metric_id,
            "metric_code": metric.get("指标编号"),
            "source_module": "realtime_metrics",
            "target_module": "ttl_policy",
            "result_table": metric.get("输出表名"),
            # 保留当前版本：交付的是结果表当前版本与已消费版本
            "result_table_version": table_version,
            "consumed_version": int(cursor["consumed_version"]),
            "topics": topics,
            "row_count": result_count,
            "delivered_at": datetime.now().isoformat(),
            "status": "已交付",
        }
        store.rows(DELIVERY_TABLE).append(delivery)
        return {"ok": True, "message": "指标增量结果表已交付数据生命周期", "delivery": delivery, "duplicated": False}

    def list_deliveries(self, metric_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.rows(DELIVERY_TABLE)
        if metric_id is not None:
            rows = [r for r in rows if r.get("metric_id") == metric_id]
        return [dict(r) for r in rows]

    def retention_check(self, metric_id: int | None = None) -> dict[str, Any]:
        """数据生命周期口径：判断结果表涉及的主题是否仍处于可消费保留期。

        以水位追踪的检查时间作为可消费参考时间，主题保留时长（topic_registry.保留时长）
        之内的窗口仍可消费；超期或缺少水位的主题标记为不可消费/待核对。
        """
        result_rows = store.rows(METRIC_RESULTS_TABLE)
        if metric_id is not None:
            result_rows = [r for r in result_rows if r.get("metric_id") == metric_id]

        topics: dict[str, dict[str, Any]] = {}
        for row in result_rows:
            code = row.get("topic_code")
            if code and code not in topics:
                topics[code] = {
                    "topic_code": code,
                    "latest_window_start": row.get("window_start"),
                    "window_count": 0,
                }
            if code:
                topics[code]["window_count"] += 1
                if row.get("window_start", "") > topics[code]["latest_window_start"]:
                    topics[code]["latest_window_start"] = row.get("window_start")

        now = datetime.now()
        checks: list[dict[str, Any]] = []
        for code, info in sorted(topics.items()):
            topic = next((r for r in store.rows("topic_registry") if r.get("主题编号") == code), None)
            watermark = next((r for r in store.rows("watermark_track") if r.get("数据主题") == code), None)
            retention_days = _parse_duration_days(topic.get("保留时长")) if topic else None

            reference_time: datetime | None = None
            if watermark and watermark.get("检查时间") not in (None, "-"):
                try:
                    reference_time = datetime.fromisoformat(str(watermark["检查时间"]))
                except ValueError:
                    reference_time = None

            consumable: bool | None
            reason: str
            if topic is None:
                consumable, reason = None, "主题登记缺失，待核对"
            elif retention_days is None:
                consumable, reason = None, "保留时长无法解析，待核对"
            elif reference_time is None:
                consumable, reason = None, "缺少水位检查时间，待核对"
            else:
                age_days = (now - reference_time).days
                consumable = age_days <= retention_days
                reason = (
                    f"水位参考时间距今 {age_days} 天，保留 {retention_days} 天"
                    if consumable
                    else f"已超出 {retention_days} 天保留期（距今 {age_days} 天）"
                )

            checks.append({
                **info,
                "topic_name": topic.get("主题名称") if topic else None,
                "retention": topic.get("保留时长") if topic else None,
                "retention_days": retention_days,
                "watermark_check_time": watermark.get("检查时间") if watermark else None,
                "consumable": consumable,
                "reason": reason,
            })

        return {
            "ok": True,
            "checked_at": now.isoformat(),
            "total_topics": len(checks),
            "consumable_topics": sum(1 for c in checks if c["consumable"] is True),
            "expired_topics": sum(1 for c in checks if c["consumable"] is False),
            "pending_topics": sum(1 for c in checks if c["consumable"] is None),
            "items": checks,
        }


# 单例：与 pipeline_engine 的用法保持一致
metric_increment_service = MetricIncrementService()
