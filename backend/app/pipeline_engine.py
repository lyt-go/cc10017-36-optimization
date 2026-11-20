"""数据管道引擎：实现跨模块的真实数据流转与链路编排。

核心链路：
1. 实时接入链：主题注册 → Kafka接入 → Schema演进 → Flink计算
2. CDC入湖链：CDC捕获 → UPSERT合并 → 湖分区 → Iceberg管理
3. 质量容错链：水位追踪 → 流式质量 → 死信处理
4. 数据消费链：流式血缘 → 物化视图 → 下沉分发
5. 维护链：流式补数 → 合并清理 → 生命周期
6. 实时指标链：消费主题+Flink结果 → 实时指标
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from app.store import store
from app.services.metric_increment import (
    IncrementWriteInterrupted,
    metric_increment_service,
)


class PipelineEngine:
    """编排多模块数据链，维护链路状态与产物。"""

    # 链路类型常量
    INGEST_CHAIN = "realtime_ingest"
    CDC_CHAIN = "cdc_to_lake"
    QUALITY_CHAIN = "quality_tolerance"
    CONSUME_CHAIN = "data_consumption"
    MAINTAIN_CHAIN = "maintenance"
    METRICS_CHAIN = "realtime_metrics"

    def __init__(self) -> None:
        self._pipelines: dict[str, list[dict[str, Any]]] = {}

    # ───────────────────────────────
    # 实时接入链
    # ───────────────────────────────

    def ingest_chain_status(self, topic_code: str) -> dict[str, Any]:
        """查询实时接入链全链路状态：主题→Kafka→Schema→Flink。"""
        topic = self._find_by_field("topic_registry", "主题编号", topic_code)
        kafka = self._find_by_field("kafka_connector", "配置编号", topic_code.replace("TOPI", "KAFK")) if topic else None
        schema = self._find_by_field("schema_evolve", "关联主题", topic_code) if topic else None
        flink = self._find_by_field("flink_job", "消费主题", topic_code) if topic else None

        return {
            "topic": {"exists": topic is not None, "status": topic.get("status") if topic else None, "entry": topic},
            "kafka_connector": {"exists": kafka is not None, "status": kafka.get("status") if kafka else None, "entry": kafka},
            "schema_evolve": {"exists": schema is not None, "status": schema.get("status") if schema else None, "entry": schema},
            "flink_job": {"exists": flink is not None, "status": flink.get("status") if flink else None, "entry": flink},
            "chain_ready": all([
                topic is not None and topic.get("status") == "已上线",
                kafka is not None and kafka.get("status") == "已生效",
                schema is not None and schema.get("status") == "已生效",
                flink is not None and flink.get("status") == "运行中",
            ]),
        }

    def create_ingest_pipeline(self, topic_code: str, topic_name: str, msg_format: str) -> dict[str, Any]:
        """为主题创建完整的实时接入链路记录。"""
        pipeline_id = f"INGEST-{topic_code}"
        entry = {
            "pipeline_id": pipeline_id,
            "chain_type": self.INGEST_CHAIN,
            "topic_code": topic_code,
            "created_at": datetime.now().isoformat(),
            "stages": [
                {"module": "topic_registry", "status": "草稿", "artifact": topic_code},
                {"module": "kafka_connector", "status": "待创建", "artifact": None},
                {"module": "schema_evolve", "status": "待创建", "artifact": None},
                {"module": "flink_job", "status": "待创建", "artifact": None},
            ],
            "overall_status": "草稿",
        }
        self._pipelines.setdefault(self.INGEST_CHAIN, []).append(entry)
        return entry

    def advance_ingest_stage(self, topic_code: str, stage_module: str, new_status: str, artifact: str | None = None) -> dict[str, Any] | None:
        """推进实时接入链的某个阶段状态。"""
        for pipe in self._pipelines.get(self.INGEST_CHAIN, []):
            if pipe.get("topic_code") == topic_code:
                for stage in pipe["stages"]:
                    if stage["module"] == stage_module:
                        stage["status"] = new_status
                        if artifact:
                            stage["artifact"] = artifact
                        break
                # 重新计算整体状态
                statuses = [s["status"] for s in pipe["stages"]]
                if all(s == "已完成" for s in statuses):
                    pipe["overall_status"] = "运行中"
                elif any(s in ("失败", "异常") for s in statuses):
                    pipe["overall_status"] = "异常"
                else:
                    pipe["overall_status"] = "进行中"
                return pipe
        return None

    # ───────────────────────────────
    # CDC入湖链
    # ───────────────────────────────

    def cdc_chain_status(self, source_db: str, source_table: str) -> dict[str, Any]:
        """查询CDC入湖链：CDC→UPSERT→分区→Iceberg。"""
        cdc = self._find_by_field("change_capture", "源数据库", source_db)
        if cdc and cdc.get("源表名") != source_table:
            cdc = None
        target_topic = cdc.get("目标主题") if cdc else None
        upsert = self._find_by_field("upsert_merge", "目标表名", source_table) if cdc else None
        partition = self._find_by_field("lake_partition", "目标表名", source_table) if cdc else None
        iceberg = self._find_by_field("iceberg_table", "表名称", source_table) if cdc else None

        return {
            "cdc": {"exists": cdc is not None, "status": cdc.get("status") if cdc else None, "entry": cdc},
            "upsert_merge": {"exists": upsert is not None, "status": upsert.get("status") if upsert else None, "entry": upsert},
            "lake_partition": {"exists": partition is not None, "status": partition.get("status") if partition else None, "entry": partition},
            "iceberg_table": {"exists": iceberg is not None, "status": iceberg.get("status") if iceberg else None, "entry": iceberg},
            "target_topic": target_topic,
            "chain_ready": all([
                cdc is not None and cdc.get("status") == "运行中",
                upsert is not None and upsert.get("status") == "已生效",
                partition is not None and partition.get("status") == "已生效",
                iceberg is not None and iceberg.get("status") in ("已就绪", "运行中"),
            ]),
        }

    def run_cdc_upsert_simulation(self, cdc_id: int) -> dict[str, Any]:
        """模拟CDC捕获后的增量合并：产生合并结果记录。"""
        cdc = store.find("change_capture", cdc_id)
        if not cdc:
            return {"ok": False, "error": f"CDC配置 {cdc_id} 不存在"}

        source_table = cdc.get("源表名", "unknown")
        upsert = self._find_by_field("upsert_merge", "目标表名", source_table)
        if not upsert:
            return {"ok": False, "error": f"源表 {source_table} 未配置合并策略"}

        merge_key = upsert.get("合并键列", "id")
        # 模拟产生合并产物
        merged_records = []
        for i in range(3):
            merged_records.append({
                "merge_key": f"{merge_key}-{(i+1):04d}",
                "operation": ["INSERT", "UPDATE", "DELETE"][i % 3],
                "source_table": source_table,
                "cdc_config_id": cdc_id,
                "merged_at": datetime.now().isoformat(),
            })

        # 写入upsert_merge模块的附属产物表
        self._ensure_artifact_table("upsert_merge_artifacts")
        for rec in merged_records:
            store.rows("upsert_merge_artifacts").append(rec)

        return {
            "ok": True,
            "cdc_id": cdc_id,
            "source_table": source_table,
            "merge_key": merge_key,
            "merged_count": len(merged_records),
            "artifacts": merged_records,
        }

    def apply_partition_to_iceberg(self, table_name: str) -> dict[str, Any]:
        """将分区策略应用到Iceberg表：跨模块数据产物。"""
        partition = self._find_by_field("lake_partition", "目标表名", table_name)
        iceberg = self._find_by_field("iceberg_table", "表名称", table_name)
        if not partition or not iceberg:
            return {"ok": False, "error": "分区定义或Iceberg表缺失"}

        # 生成分区元数据产物
        partition_spec = {
            "table_name": table_name,
            "partition_column": partition.get("分区列名"),
            "granularity": partition.get("分区粒度"),
            "format": partition.get("文件格式"),
            "iceberg_metadata_version": iceberg.get("元数据版本"),
            "applied_at": datetime.now().isoformat(),
        }
        self._ensure_artifact_table("iceberg_partition_specs")
        store.rows("iceberg_partition_specs").append(partition_spec)

        return {"ok": True, "spec": partition_spec}

    # ───────────────────────────────
    # 质量容错链
    # ───────────────────────────────

    def quality_chain_check(self, topic_code: str) -> dict[str, Any]:
        """检查质量容错链状态：水位→质量规则→死信。"""
        watermark = self._find_by_field("watermark_track", "数据主题", topic_code)
        quality = self._find_by_field("stream_quality", "监控主题", topic_code)
        dead_letters = [r for r in store.rows("dead_letter") if r.get("原始主题") == topic_code]

        # 计算质量指标
        late_count = int(watermark.get("延迟记录数", 0)) if watermark else 0
        quality_threshold = quality.get("告警阈值", "0") if quality else "0"
        try:
            threshold_val = float(quality_threshold)
        except (ValueError, TypeError):
            threshold_val = 0.0

        quality_alert = late_count > threshold_val

        return {
            "watermark": {"exists": watermark is not None, "status": watermark.get("status") if watermark else None,
                          "late_count": late_count, "entry": watermark},
            "stream_quality": {"exists": quality is not None, "status": quality.get("status") if quality else None,
                               "threshold": threshold_val, "entry": quality},
            "dead_letters": {"count": len(dead_letters), "entries": dead_letters},
            "quality_alert": quality_alert,
            "chain_healthy": watermark is not None and quality is not None and not quality_alert and len(dead_letters) < 3,
        }

    def run_quality_inspection(self, topic_code: str) -> dict[str, Any]:
        """执行质量巡检：基于水位和规则产生检查结果。"""
        watermark = self._find_by_field("watermark_track", "数据主题", topic_code)
        quality = self._find_by_field("stream_quality", "监控主题", topic_code)

        if not watermark or not quality:
            return {"ok": False, "error": "水位追踪或质量规则未配置"}

        late_count = int(watermark.get("延迟记录数", 0))
        try:
            threshold = float(quality.get("告警阈值", "0"))
        except (ValueError, TypeError):
            threshold = 0.0

        violations = []
        if late_count > threshold:
            violations.append({
                "type": "延迟超限",
                "detail": f"延迟记录 {late_count} 超过阈值 {threshold}",
                "topic": topic_code,
                "timestamp": datetime.now().isoformat(),
            })

        # 如果存在死信，也计入 violations
        dead_letters = [r for r in store.rows("dead_letter") if r.get("原始主题") == topic_code]
        for dl in dead_letters:
            if dl.get("处置状态") == "待处置":
                violations.append({
                    "type": "死信待处置",
                    "detail": f"死信记录 {dl.get('记录编号')} 待处置",
                    "topic": topic_code,
                    "timestamp": dl.get("入库时间"),
                })

        # 写入质量检查结果产物
        self._ensure_artifact_table("quality_inspection_results")
        result = {
            "topic": topic_code,
            "inspected_at": datetime.now().isoformat(),
            "violation_count": len(violations),
            "violations": violations,
            "overall": "通过" if not violations else "告警",
        }
        store.rows("quality_inspection_results").append(result)

        return {"ok": True, "result": result}

    # ───────────────────────────────
    # 数据消费链
    # ───────────────────────────────

    def consumption_chain_status(self, upstream_topic: str) -> dict[str, Any]:
        """查询数据消费链：血缘→物化视图→下沉分发。"""
        lineage = self._find_by_field("stream_lineage", "上游主题", upstream_topic)
        downstream_table = lineage.get("下游表名") if lineage else None
        mv = self._find_by_field("materialized_view", "基表名", downstream_table) if downstream_table else None
        sink = self._find_by_field("sink_dispatch", "湖表名", downstream_table) if downstream_table else None

        return {
            "lineage": {"exists": lineage is not None, "status": lineage.get("status") if lineage else None, "entry": lineage},
            "materialized_view": {"exists": mv is not None, "status": mv.get("status") if mv else None, "entry": mv},
            "sink_dispatch": {"exists": sink is not None, "status": sink.get("status") if sink else None, "entry": sink},
            "downstream_table": downstream_table,
            "chain_ready": all([
                lineage is not None and lineage.get("status") == "已生效",
                mv is not None and mv.get("status") == "已启用",
                sink is not None and sink.get("status") == "已生效",
            ]),
        }

    def refresh_materialized_from_lineage(self, lineage_id: int) -> dict[str, Any]:
        """根据血缘记录刷新物化视图：血缘驱动物化查询。"""
        lineage = store.find("stream_lineage", lineage_id)
        if not lineage:
            return {"ok": False, "error": f"血缘记录 {lineage_id} 不存在"}

        upstream = lineage.get("上游主题")
        downstream = lineage.get("下游表名")
        field_map = lineage.get("字段映射", "")

        # 查找关联的物化视图
        mv = self._find_by_field("materialized_view", "基表名", downstream)
        if not mv:
            return {"ok": False, "error": f"下游表 {downstream} 未配置物化视图"}

        # 模拟生成刷新产物
        refreshed_data = {
            "lineage_id": lineage_id,
            "upstream_topic": upstream,
            "downstream_table": downstream,
            "field_mapping": field_map,
            "mv_id": mv.get("id"),
            "refreshed_at": datetime.now().isoformat(),
            "record_count": 42,  # 模拟值
        }
        self._ensure_artifact_table("materialized_refresh_log")
        store.rows("materialized_refresh_log").append(refreshed_data)

        # 同时触发下沉分发检查
        sink = self._find_by_field("sink_dispatch", "湖表名", downstream)
        if sink:
            dispatch_record = {
                "mv_refresh_id": refreshed_data["refreshed_at"],
                "sink_rule_id": sink.get("id"),
                "target_system": sink.get("目标系统"),
                "dispatched_at": datetime.now().isoformat(),
                "status": "待推送",
            }
            self._ensure_artifact_table("sink_dispatch_queue")
            store.rows("sink_dispatch_queue").append(dispatch_record)
            refreshed_data["dispatch_queued"] = True
        else:
            refreshed_data["dispatch_queued"] = False

        return {"ok": True, "refreshed": refreshed_data}

    # ───────────────────────────────
    # 维护链
    # ───────────────────────────────

    def maintenance_chain_status(self, target_table: str) -> dict[str, Any]:
        """查询维护链：补数→合并清理→生命周期。"""
        backfill = self._find_by_field("backfill_stream", "目标表名", target_table)
        compact = self._find_by_field("compact_policy", "目标表名", target_table)
        ttl = self._find_by_field("ttl_policy", "目标表名", target_table)

        return {
            "backfill": {"exists": backfill is not None, "status": backfill.get("status") if backfill else None, "entry": backfill},
            "compact_policy": {"exists": compact is not None, "status": compact.get("status") if compact else None, "entry": compact},
            "ttl_policy": {"exists": ttl is not None, "status": ttl.get("status") if ttl else None, "entry": ttl},
            "chain_ready": all([
                backfill is not None and backfill.get("status") == "已完成",
                compact is not None and compact.get("status") == "已生效",
                ttl is not None and ttl.get("status") == "已生效",
            ]),
        }

    def run_backfill_then_compact(self, backfill_id: int) -> dict[str, Any]:
        """执行补数后触发合并清理：维护链联动。"""
        backfill = store.find("backfill_stream", backfill_id)
        if not backfill:
            return {"ok": False, "error": f"补数任务 {backfill_id} 不存在"}

        target_table = backfill.get("目标表名")
        compact = self._find_by_field("compact_policy", "目标表名", target_table)
        if not compact:
            return {"ok": False, "error": f"表 {target_table} 未配置合并策略"}

        # 模拟补数过程
        start_offset = backfill.get("起始偏移", "0")
        end_offset = backfill.get("截止偏移", "0")
        try:
            start_i = int(start_offset)
            end_i = int(end_offset)
        except (ValueError, TypeError):
            start_i, end_i = 0, 100

        backfilled_count = max(0, end_i - start_i)

        # 更新补数任务状态
        backfill["status"] = "已完成"
        backfill["pending"] = False

        # 触发合并产物
        compact_result = {
            "backfill_id": backfill_id,
            "target_table": target_table,
            "backfilled_count": backfilled_count,
            "compact_policy_id": compact.get("id"),
            "merged_files": backfilled_count // 10 + 1,
            "compacted_at": datetime.now().isoformat(),
        }
        self._ensure_artifact_table("compact_results")
        store.rows("compact_results").append(compact_result)

        # 检查生命周期策略
        ttl = self._find_by_field("ttl_policy", "目标表名", target_table)
        if ttl:
            ttl_record = {
                "table": target_table,
                "hot_retention": ttl.get("热数据保留"),
                "warm_window": ttl.get("温数据窗口"),
                "cold_archive": ttl.get("冷归档策略"),
                "applied_at": datetime.now().isoformat(),
            }
            self._ensure_artifact_table("ttl_applied_log")
            store.rows("ttl_applied_log").append(ttl_record)
            compact_result["ttl_applied"] = ttl_record
        else:
            compact_result["ttl_applied"] = None

        return {"ok": True, "result": compact_result}

    # ───────────────────────────────
    # 实时指标链
    # ───────────────────────────────

    def compute_realtime_metrics(self, metric_id: int) -> dict[str, Any]:
        """计算实时指标：消费主题注册数据与Flink计算结果。"""
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        output_table = metric.get("输出表名")
        agg_window = metric.get("聚合窗口", "1h")
        agg_func = metric.get("聚合函数", "count")

        # 查找关联的主题和Flink作业
        related_topics = [r for r in store.rows("topic_registry") if r.get("status") == "已上线"]
        related_flink = [r for r in store.rows("flink_job") if r.get("status") == "运行中"]

        # 模拟指标计算
        topic_count = len(related_topics)
        flink_count = len(related_flink)

        if agg_func == "count":
            computed_value = topic_count + flink_count
        elif agg_func == "avg":
            computed_value = (topic_count + flink_count) / 2.0 if (topic_count + flink_count) > 0 else 0
        elif agg_func == "sum":
            computed_value = topic_count * 100 + flink_count * 50
        else:
            computed_value = topic_count

        result = {
            "metric_id": metric_id,
            "metric_name": metric.get("指标名称"),
            "output_table": output_table,
            "agg_window": agg_window,
            "agg_func": agg_func,
            "computed_value": computed_value,
            "source_topics": topic_count,
            "source_flink_jobs": flink_count,
            "computed_at": datetime.now().isoformat(),
        }

        self._ensure_artifact_table("realtime_metric_results")
        store.rows("realtime_metric_results").append(result)

        return {"ok": True, "result": result}

    # ───────────────────────────────────────
    # 实时指标链：增量结果表（增量计算/中断恢复/跨模块交付）
    # ───────────────────────────────────────

    def ingest_metric_versions(self, metric_id: int, count: int = 1) -> dict[str, Any]:
        """接入订单吞吐指标的短窗口源版本（新版本只取数一次，不重扫历史）。"""
        return metric_increment_service.ingest_source_versions(metric_id, count)

    def refresh_metric_increment(self, metric_id: int, *, fail_after: int | None = None) -> dict[str, Any]:
        """增量刷新：从上次已消费版本之后连续推进，写入新增聚合值。"""
        try:
            return metric_increment_service.refresh_increment(metric_id, fail_after=fail_after)
        except IncrementWriteInterrupted as exc:
            return exc.partial

    def recover_metric_increment(self, metric_id: int) -> dict[str, Any]:
        """中断恢复：补齐未消费版本，已写窗口去重跳过。"""
        return metric_increment_service.recover_increment(metric_id)

    def deliver_metric_increment(self, metric_id: int) -> dict[str, Any]:
        """跨模块交付：实时指标 → 数据生命周期，保留结果表当前版本。"""
        return metric_increment_service.deliver_to_lifecycle(metric_id)

    def metric_retention_check(self, metric_id: int | None = None) -> dict[str, Any]:
        """数据生命周期口径：核对来源主题是否仍在可消费保留期。"""
        return metric_increment_service.retention_check(metric_id)

    # ───────────────────────────────
    # 链路血缘与依赖图
    # ───────────────────────────────

    def build_full_lineage_graph(self) -> dict[str, Any]:
        """构建全平台数据血缘图：包含所有模块间的数据依赖。"""
        nodes: list[dict] = []
        edges: list[dict] = []

        # 主题注册节点
        for row in store.rows("topic_registry"):
            if row.get("status") == "已上线":
                nodes.append({
                    "id": f"topic-{row['id']}",
                    "name": row.get("主题名称"),
                    "type": "topic",
                    "module": "topic_registry",
                    "status": row.get("status"),
                })

        # Kafka节点
        for row in store.rows("kafka_connector"):
            if row.get("status") == "已生效":
                nodes.append({
                    "id": f"kafka-{row['id']}",
                    "name": row.get("配置编号"),
                    "type": "connector",
                    "module": "kafka_connector",
                    "status": row.get("status"),
                })

        # Flink节点
        for row in store.rows("flink_job"):
            if row.get("status") == "运行中":
                nodes.append({
                    "id": f"flink-{row['id']}",
                    "name": row.get("任务编号"),
                    "type": "job",
                    "module": "flink_job",
                    "status": row.get("status"),
                })

        # CDC节点
        for row in store.rows("change_capture"):
            if row.get("status") == "运行中":
                nodes.append({
                    "id": f"cdc-{row['id']}",
                    "name": row.get("配置编号"),
                    "type": "cdc",
                    "module": "change_capture",
                    "status": row.get("status"),
                })

        # Iceberg节点
        for row in store.rows("iceberg_table"):
            if row.get("status") in ("已就绪", "运行中"):
                nodes.append({
                    "id": f"iceberg-{row['id']}",
                    "name": row.get("表名称"),
                    "type": "table",
                    "module": "iceberg_table",
                    "status": row.get("status"),
                })

        # 物化视图节点
        for row in store.rows("materialized_view"):
            if row.get("status") == "已启用":
                nodes.append({
                    "id": f"mv-{row['id']}",
                    "name": row.get("视图编号"),
                    "type": "view",
                    "module": "materialized_view",
                    "status": row.get("status"),
                })

        # 构建边：基于流式血缘记录
        for row in store.rows("stream_lineage"):
            if row.get("status") == "已生效":
                upstream_topic = row.get("上游主题")
                downstream_table = row.get("下游表名")
                # 查找对应节点id
                upstream_node = next((n for n in nodes if n["name"] == upstream_topic), None)
                downstream_node = next((n for n in nodes if n["name"] == downstream_table), None)
                if upstream_node and downstream_node:
                    edges.append({
                        "from": upstream_node["id"],
                        "to": downstream_node["id"],
                        "label": row.get("转换节点", "transform"),
                        "field_map": row.get("字段映射"),
                    })

        # 补充边：topic -> kafka -> flink
        topic_nodes = {n["name"]: n["id"] for n in nodes if n["type"] == "topic"}
        for row in store.rows("kafka_connector"):
            if row.get("status") == "已生效":
                kafka_id = f"kafka-{row['id']}"
                # 假设kafka消费的主题
                for topic_name, topic_id in topic_nodes.items():
                    edges.append({"from": topic_id, "to": kafka_id, "label": "consume"})

        flink_nodes = {n["name"]: n["id"] for n in nodes if n["type"] == "job"}
        for row in store.rows("flink_job"):
            if row.get("status") == "运行中":
                flink_id = f"flink-{row['id']}"
                consumed_topic = row.get("消费主题")
                if consumed_topic in topic_nodes:
                    edges.append({"from": topic_nodes[consumed_topic], "to": flink_id, "label": "compute"})

        return {"nodes": nodes, "edges": edges}

    # ───────────────────────────────
    # 链路总览
    # ───────────────────────────────

    def pipeline_overview(self) -> dict[str, Any]:
        """返回所有链路的运营总览。"""
        chains = {
            self.INGEST_CHAIN: self._count_chain_ready(self.INGEST_CHAIN),
            self.CDC_CHAIN: self._count_cdc_ready(),
            self.QUALITY_CHAIN: self._count_quality_ready(),
            self.CONSUME_CHAIN: self._count_consume_ready(),
            self.MAINTAIN_CHAIN: self._count_maintain_ready(),
            self.METRICS_CHAIN: self._count_metrics_ready(),
        }

        artifacts = {
            "upsert_merge_artifacts": len(store.rows("upsert_merge_artifacts")),
            "iceberg_partition_specs": len(store.rows("iceberg_partition_specs")),
            "quality_inspection_results": len(store.rows("quality_inspection_results")),
            "materialized_refresh_log": len(store.rows("materialized_refresh_log")),
            "sink_dispatch_queue": len(store.rows("sink_dispatch_queue")),
            "compact_results": len(store.rows("compact_results")),
            "ttl_applied_log": len(store.rows("ttl_applied_log")),
            "realtime_metric_results": len(store.rows("realtime_metric_results")),
            "metric_increment_results": len(store.rows("metric_increment_results")),
            "metric_increment_cursors": len(store.rows("metric_increment_cursors")),
            "metric_source_versions": len(store.rows("metric_source_versions")),
            "ttl_delivery_log": len(store.rows("ttl_delivery_log")),
        }

        return {
            "chains": chains,
            "artifact_counts": artifacts,
            "total_pipelines": sum(len(v) for v in self._pipelines.values()),
        }

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _find_by_field(self, module: str, field: str, value: str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        for row in store.rows(module):
            if str(row.get(field, "")) == str(value):
                return row
        return None

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)  # store.rows会setdefault

    def _count_chain_ready(self, chain_type: str) -> dict[str, int]:
        pipes = self._pipelines.get(chain_type, [])
        ready = sum(1 for p in pipes if p.get("overall_status") == "运行中")
        return {"total": len(pipes), "ready": ready}

    def _count_cdc_ready(self) -> dict[str, int]:
        cdc_rows = [r for r in store.rows("change_capture") if r.get("status") == "运行中"]
        ready = 0
        for cdc in cdc_rows:
            table = cdc.get("源表名")
            upsert = self._find_by_field("upsert_merge", "目标表名", table)
            partition = self._find_by_field("lake_partition", "目标表名", table)
            iceberg = self._find_by_field("iceberg_table", "表名称", table)
            if upsert and partition and iceberg:
                ready += 1
        return {"total": len(cdc_rows), "ready": ready}

    def _count_quality_ready(self) -> dict[str, int]:
        wm_rows = store.rows("watermark_track")
        ready = 0
        for wm in wm_rows:
            topic = wm.get("数据主题")
            quality = self._find_by_field("stream_quality", "监控主题", topic)
            if quality:
                ready += 1
        return {"total": len(wm_rows), "ready": ready}

    def _count_consume_ready(self) -> dict[str, int]:
        lineage_rows = [r for r in store.rows("stream_lineage") if r.get("status") == "已生效"]
        ready = 0
        for lin in lineage_rows:
            table = lin.get("下游表名")
            mv = self._find_by_field("materialized_view", "基表名", table)
            sink = self._find_by_field("sink_dispatch", "湖表名", table)
            if mv and sink:
                ready += 1
        return {"total": len(lineage_rows), "ready": ready}

    def _count_maintain_ready(self) -> dict[str, int]:
        bf_rows = store.rows("backfill_stream")
        ready = 0
        for bf in bf_rows:
            table = bf.get("目标表名")
            compact = self._find_by_field("compact_policy", "目标表名", table)
            ttl = self._find_by_field("ttl_policy", "目标表名", table)
            if compact and ttl:
                ready += 1
        return {"total": len(bf_rows), "ready": ready}

    def _count_metrics_ready(self) -> dict[str, int]:
        metrics = store.rows("realtime_metrics")
        ready = sum(1 for m in metrics if m.get("status") == "已生效")
        return {"total": len(metrics), "ready": ready}


pipeline_engine = PipelineEngine()
