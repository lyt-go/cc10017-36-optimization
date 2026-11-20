"""指标增量结果表接口：增量刷新、中断恢复、结果表查询与跨模块交付。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.metric_increment import (
    IncrementWriteInterrupted,
    metric_increment_service,
)

router = APIRouter(prefix="/api/metric_increment", tags=["指标增量结果表"])


class IngestPayload(BaseModel):
    count: int = 1


class InterruptPayload(BaseModel):
    # 写入多少个新窗口后模拟中断
    fail_after: int = 1


@router.post("/metrics/{metric_id}/ingest")
def ingest_versions(metric_id: int, payload: IngestPayload) -> dict[str, Any]:
    """模拟短窗口源版本到达（接入新窗口，不重扫历史）。"""
    return metric_increment_service.ingest_source_versions(metric_id, max(1, payload.count))


@router.get("/metrics/{metric_id}/source-versions")
def list_source_versions(metric_id: int) -> dict[str, Any]:
    return {"items": metric_increment_service.list_source_versions(metric_id)}


@router.get("/metrics/{metric_id}/cursor")
def get_cursor(metric_id: int) -> dict[str, Any]:
    """查看已消费版本与结果表当前版本。"""
    return metric_increment_service.get_cursor(metric_id)


@router.post("/metrics/{metric_id}/refresh")
def refresh_increment(metric_id: int) -> dict[str, Any]:
    """增量刷新：只处理已消费版本之后的连续新窗口。"""
    return metric_increment_service.refresh_increment(metric_id)


@router.post("/metrics/{metric_id}/interrupt")
def simulate_interrupt(metric_id: int, payload: InterruptPayload) -> dict[str, Any]:
    """模拟写入中断：部分窗口落表但游标不推进，供恢复演练。"""
    try:
        return metric_increment_service.refresh_increment(
            metric_id, fail_after=max(1, payload.fail_after)
        )
    except IncrementWriteInterrupted as exc:
        return exc.partial


@router.post("/metrics/{metric_id}/recover")
def recover_increment(metric_id: int) -> dict[str, Any]:
    """中断恢复：补齐未消费版本，已写窗口自动跳过，不重复累计。"""
    return metric_increment_service.recover_increment(metric_id)


@router.get("/results")
def list_results(
    metric_id: int | None = Query(default=None),
    window_start: str | None = Query(default=None),
    window_end: str | None = Query(default=None),
    only_new: bool = Query(default=False),
) -> dict[str, Any]:
    """查询指标增量结果表，支持按时间窗口筛选、只看新增聚合值。"""
    items = metric_increment_service.list_results(
        metric_id=metric_id,
        window_start=window_start,
        window_end=window_end,
        only_new=only_new,
    )
    return {"items": items, "total": len(items)}


@router.post("/metrics/{metric_id}/deliver")
def deliver_to_lifecycle(metric_id: int) -> dict[str, Any]:
    """跨模块：把结果表当前版本交付给数据生命周期（幂等）。"""
    return metric_increment_service.deliver_to_lifecycle(metric_id)


@router.get("/deliveries")
def list_deliveries(metric_id: int | None = Query(default=None)) -> dict[str, Any]:
    return {"items": metric_increment_service.list_deliveries(metric_id)}


@router.get("/retention")
def retention_check(metric_id: int | None = Query(default=None)) -> dict[str, Any]:
    """数据生命周期口径：核对来源主题是否仍在可消费保留期。"""
    return metric_increment_service.retention_check(metric_id)
