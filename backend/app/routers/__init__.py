"""业务模块路由汇总。

这里统一按别名导入再暴露 ROUTERS：模块名有可能和内置名撞车（某个业务模块就叫 dict、list
这种名字时），按名字直接 import 会把内置类型覆盖掉，函数注解在运行时求值就会报
'module' object is not subscriptable。
"""
from __future__ import annotations

from app.routers import topic_registry as router_topic_registry
from app.routers import flink_job as router_flink_job
from app.routers import kafka_connector as router_kafka_connector
from app.routers import schema_evolve as router_schema_evolve
from app.routers import upsert_merge as router_upsert_merge
from app.routers import lake_partition as router_lake_partition
from app.routers import stream_quality as router_stream_quality
from app.routers import iceberg_table as router_iceberg_table
from app.routers import watermark_track as router_watermark_track
from app.routers import dead_letter as router_dead_letter
from app.routers import materialized_view as router_materialized_view
from app.routers import change_capture as router_change_capture
from app.routers import compact_policy as router_compact_policy
from app.routers import stream_lineage as router_stream_lineage
from app.routers import backfill_stream as router_backfill_stream
from app.routers import sink_dispatch as router_sink_dispatch
from app.routers import ttl_policy as router_ttl_policy
from app.routers import realtime_metrics as router_realtime_metrics
from app.routers import metric_increment as router_metric_increment
from app.routers import pipeline as router_pipeline

ROUTERS = [router_topic_registry, router_flink_job, router_kafka_connector, router_schema_evolve, router_upsert_merge, router_lake_partition, router_stream_quality, router_iceberg_table, router_watermark_track, router_dead_letter, router_materialized_view, router_change_capture, router_compact_policy, router_stream_lineage, router_backfill_stream, router_sink_dispatch, router_ttl_policy, router_realtime_metrics, router_metric_increment, router_pipeline]
