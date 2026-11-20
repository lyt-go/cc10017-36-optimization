"""数据管道链路接口：暴露跨模块数据链的查询与执行能力。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.pipeline_engine import pipeline_engine
from app.schemas import ActionResult

router = APIRouter(prefix="/api/pipeline", tags=["数据管道"])


@router.get("/ingest/{topic_code}")
def get_ingest_chain(topic_code: str) -> dict:
    """查询实时接入链状态：主题→Kafka→Schema→Flink。"""
    return pipeline_engine.ingest_chain_status(topic_code)


@router.post("/ingest/{topic_code}/advance")
def advance_ingest_stage(topic_code: str, payload: dict) -> ActionResult:
    """推进实时接入链阶段。"""
    stage = payload.get("stage")
    status = payload.get("status")
    artifact = payload.get("artifact")
    if not stage or not status:
        return ActionResult(ok=False, message="缺少 stage 或 status")
    result = pipeline_engine.advance_ingest_stage(topic_code, stage, status, artifact)
    if result is None:
        return ActionResult(ok=False, message=f"接入链 {topic_code} 不存在")
    return ActionResult(ok=True, message="阶段已推进", entry=result)


@router.get("/cdc/{source_db}/{source_table}")
def get_cdc_chain(source_db: str, source_table: str) -> dict:
    """查询CDC入湖链：CDC→UPSERT→分区→Iceberg。"""
    return pipeline_engine.cdc_chain_status(source_db, source_table)


@router.post("/cdc/{cdc_id}/upsert")
def run_cdc_upsert(cdc_id: int) -> ActionResult:
    """执行CDC增量合并模拟。"""
    result = pipeline_engine.run_cdc_upsert_simulation(cdc_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=f"CDC {cdc_id} 增量合并完成", entry=result)


@router.post("/partition/apply/{table_name}")
def apply_partition(table_name: str) -> ActionResult:
    """将分区策略应用到Iceberg表。"""
    result = pipeline_engine.apply_partition_to_iceberg(table_name)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="分区策略已应用", entry=result)


@router.get("/quality/{topic_code}")
def get_quality_chain(topic_code: str) -> dict:
    """查询质量容错链：水位→质量→死信。"""
    return pipeline_engine.quality_chain_check(topic_code)


@router.post("/quality/{topic_code}/inspect")
def run_quality_inspection(topic_code: str) -> ActionResult:
    """执行质量巡检。"""
    result = pipeline_engine.run_quality_inspection(topic_code)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="质量巡检完成", entry=result)


@router.get("/consumption/{upstream_topic}")
def get_consumption_chain(upstream_topic: str) -> dict:
    """查询数据消费链：血缘→物化视图→下沉分发。"""
    return pipeline_engine.consumption_chain_status(upstream_topic)


@router.post("/consumption/lineage/{lineage_id}/refresh")
def refresh_materialized(lineage_id: int) -> ActionResult:
    """根据血缘刷新物化视图并触发下沉分发。"""
    result = pipeline_engine.refresh_materialized_from_lineage(lineage_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="物化视图已刷新", entry=result)


@router.get("/maintenance/{target_table}")
def get_maintenance_chain(target_table: str) -> dict:
    """查询维护链：补数→合并清理→生命周期。"""
    return pipeline_engine.maintenance_chain_status(target_table)


@router.post("/maintenance/backfill/{backfill_id}/compact")
def run_backfill_compact(backfill_id: int) -> ActionResult:
    """执行补数后触发合并清理与生命周期。"""
    result = pipeline_engine.run_backfill_then_compact(backfill_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="补数与合并清理完成", entry=result)


@router.post("/metrics/{metric_id}/compute")
def compute_metrics(metric_id: int) -> ActionResult:
    """计算实时指标。"""
    result = pipeline_engine.compute_realtime_metrics(metric_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="实时指标计算完成", entry=result)


@router.post("/metrics/{metric_id}/ingest")
def ingest_metric_versions(metric_id: int, payload: dict | None = None) -> ActionResult:
    """接入订单吞吐指标短窗口源版本。"""
    count = int((payload or {}).get("count", 1))
    result = pipeline_engine.ingest_metric_versions(metric_id, max(1, count))
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=f"已接入 {result.get('ingested', 0)} 个源版本", entry=result)


@router.post("/metrics/{metric_id}/increment/refresh")
def refresh_metric_increment(metric_id: int) -> ActionResult:
    """增量刷新指标结果表：只处理已消费版本之后的新窗口。"""
    result = pipeline_engine.refresh_metric_increment(metric_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=result.get("message", "增量刷新完成"), entry=result)


@router.post("/metrics/{metric_id}/increment/interrupt")
def interrupt_metric_increment(metric_id: int, payload: dict | None = None) -> ActionResult:
    """模拟写入中断：部分窗口落表、游标不推进。"""
    fail_after = int((payload or {}).get("fail_after", 1))
    result = pipeline_engine.refresh_metric_increment(metric_id, fail_after=max(1, fail_after))
    return ActionResult(
        ok=result.get("ok", False),
        message=result.get("message", "写入中断已模拟"),
        entry=result,
    )


@router.post("/metrics/{metric_id}/increment/recover")
def recover_metric_increment(metric_id: int) -> ActionResult:
    """中断恢复：补齐未消费版本，同一窗口不重复累计。"""
    result = pipeline_engine.recover_metric_increment(metric_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=result.get("message", "中断恢复完成"), entry=result)


@router.post("/metrics/{metric_id}/increment/deliver")
def deliver_metric_increment(metric_id: int) -> ActionResult:
    """跨模块交付：实时指标增量结果表 → 数据生命周期。"""
    result = pipeline_engine.deliver_metric_increment(metric_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=result.get("message", "已交付数据生命周期"), entry=result)


@router.get("/metrics/retention")
def metric_retention(metric_id: int | None = None) -> dict:
    """数据生命周期口径：核对指标来源主题的可消费保留期。"""
    return pipeline_engine.metric_retention_check(metric_id)


@router.get("/lineage-graph")
def get_full_lineage_graph() -> dict:
    """获取全平台数据血缘图。"""
    return pipeline_engine.build_full_lineage_graph()


@router.get("/overview")
def get_pipeline_overview() -> dict:
    """数据管道链路总览。"""
    return pipeline_engine.pipeline_overview()
