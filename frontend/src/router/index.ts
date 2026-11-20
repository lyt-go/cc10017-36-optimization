import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
const TopicRegistry = () => import('@/views/topic_registry/index.vue')
const FlinkJob = () => import('@/views/flink_job/index.vue')
const KafkaConnector = () => import('@/views/kafka_connector/index.vue')
const SchemaEvolve = () => import('@/views/schema_evolve/index.vue')
const UpsertMerge = () => import('@/views/upsert_merge/index.vue')
const LakePartition = () => import('@/views/lake_partition/index.vue')
const StreamQuality = () => import('@/views/stream_quality/index.vue')
const IcebergTable = () => import('@/views/iceberg_table/index.vue')
const WatermarkTrack = () => import('@/views/watermark_track/index.vue')
const DeadLetter = () => import('@/views/dead_letter/index.vue')
const MaterializedView = () => import('@/views/materialized_view/index.vue')
const ChangeCapture = () => import('@/views/change_capture/index.vue')
const CompactPolicy = () => import('@/views/compact_policy/index.vue')
const StreamLineage = () => import('@/views/stream_lineage/index.vue')
const BackfillStream = () => import('@/views/backfill_stream/index.vue')
const SinkDispatch = () => import('@/views/sink_dispatch/index.vue')
const TtlPolicy = () => import('@/views/ttl_policy/index.vue')
const RealtimeMetrics = () => import('@/views/realtime_metrics/index.vue')
const MetricWorkbench = () => import('@/views/metric_workbench/index.vue')

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/topic_registry', name: 'topic_registry', component: TopicRegistry },
    { path: '/flink_job', name: 'flink_job', component: FlinkJob },
    { path: '/kafka_connector', name: 'kafka_connector', component: KafkaConnector },
    { path: '/schema_evolve', name: 'schema_evolve', component: SchemaEvolve },
    { path: '/upsert_merge', name: 'upsert_merge', component: UpsertMerge },
    { path: '/lake_partition', name: 'lake_partition', component: LakePartition },
    { path: '/stream_quality', name: 'stream_quality', component: StreamQuality },
    { path: '/iceberg_table', name: 'iceberg_table', component: IcebergTable },
    { path: '/watermark_track', name: 'watermark_track', component: WatermarkTrack },
    { path: '/dead_letter', name: 'dead_letter', component: DeadLetter },
    { path: '/materialized_view', name: 'materialized_view', component: MaterializedView },
    { path: '/change_capture', name: 'change_capture', component: ChangeCapture },
    { path: '/compact_policy', name: 'compact_policy', component: CompactPolicy },
    { path: '/stream_lineage', name: 'stream_lineage', component: StreamLineage },
    { path: '/backfill_stream', name: 'backfill_stream', component: BackfillStream },
    { path: '/sink_dispatch', name: 'sink_dispatch', component: SinkDispatch },
    { path: '/ttl_policy', name: 'ttl_policy', component: TtlPolicy },
    { path: '/realtime_metrics', name: 'realtime_metrics', component: RealtimeMetrics },
    { path: '/metric_workbench', name: 'metric_workbench', component: MetricWorkbench },
  ],
})

export default router
