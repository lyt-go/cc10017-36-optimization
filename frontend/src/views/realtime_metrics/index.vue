<template>
  <section class="page" data-module="realtime_metrics">
    <header class="page-head">
      <div><h2>实时指标</h2><p class="page-desc">维护基于流式数据的Tumbling/Hopping/Session窗口聚合指标定义，为实时指标提供可验证的数据工程流程。</p></div>
    </header>
    <div class="analysis-summary">
      <article v-for="m in summaryMetrics" :key="m.label" class="analysis-card" data-testid="kpi-card">
        <span style="font-size:12px;color:var(--muted)">{{ m.label }}</span>
        <strong style="font-size:22px;display:block">{{ m.value }}</strong>
        <span :class="m.trend > 0 ? 'trend-up' : 'trend-down'" style="font-size:12px">{{ m.trend > 0 ? '↑' : '↓' }}{{ Math.abs(m.trend) }}%</span>
      </article>
    </div>
    <form class="analysis-form" @submit.prevent="runAnalysis">
      <label v-for="field in paramFields" :key="field"><span>{{ field }}</span>
        <select v-if="field.includes('方法')||field.includes('策略')||field.includes('粒度')" v-model="params[field]"><option v-for="o in options[field]" :key="o" :value="o">{{ o }}</option></select>
        <input v-else v-model="params[field]" :placeholder="`请输入${field}`" />
      </label>
      <button class="btn primary" type="submit" data-testid="run-analysis">运行分析</button>
    </form>
    <div v-if="chartData.length" class="analysis-chart">
      <div v-for="(bar, i) in chartData" :key="i" class="analysis-bar" :style="{ height: barHeight(bar.value) + 'px' }"><span>{{ bar.label }}</span></div>
    </div>
    <table class="data-table" v-if="results.length">
      <thead><tr><th v-for="c in resultColumns" :key="c">{{ c }}</th></tr></thead>
      <tbody><tr v-for="row in results" :key="row.id"><td v-for="c in resultColumns" :key="c">{{ row[c] ?? '—' }}</td></tr></tbody>
    </table>
    <p v-if="!results.length && !loading" style="text-align:center;color:var(--muted)">请配置参数后运行分析</p>

    <!-- 订单吞吐指标增量结果表 -->
    <div class="increment-panel">
      <h3>订单吞吐指标 · 增量结果表</h3>
      <p class="page-desc">每次刷新只处理「已消费版本」之后的连续新窗口，不再重扫全部主题与流任务；中断后按上次已消费版本补齐，同一窗口不重复累计。</p>

      <div class="stat-row">
        <article class="stat-card"><span class="stat-label">已消费版本</span><strong class="stat-value">{{ cursor.consumed_version ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">结果表版本</span><strong class="stat-value">v{{ cursor.table_version ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">结果表窗口数</span><strong class="stat-value">{{ incrementRows.length }}</strong></article>
        <article class="stat-card"><span class="stat-label">新增聚合值</span><strong class="stat-value">{{ newRowCount }}</strong></article>
      </div>

      <div class="page-actions increment-actions">
        <button class="btn" type="button" :disabled="busy" @click="ingestVersions(1)">接入1个短窗口</button>
        <button class="btn" type="button" :disabled="busy" @click="ingestVersions(3)">接入3个短窗口</button>
        <button class="btn primary" type="button" data-testid="increment-refresh" :disabled="busy" @click="refreshIncrement">增量刷新</button>
        <button class="btn" type="button" :disabled="busy" @click="simulateInterrupt">模拟写入中断</button>
        <button class="btn primary" type="button" data-testid="increment-recover" :disabled="busy" @click="recoverIncrement">中断恢复</button>
        <button class="btn" type="button" :disabled="busy" @click="deliverToLifecycle">交付数据生命周期</button>
      </div>

      <form class="filter-bar" @submit.prevent="loadIncrementRows">
        <label class="filter-item"><span>窗口起点（起）</span>
          <input v-model="windowFilter.start" data-testid="filter-window-start" placeholder="如 2026-11-16T10:02:00" />
        </label>
        <label class="filter-item"><span>窗口起点（止）</span>
          <input v-model="windowFilter.end" placeholder="如 2026-11-16T10:05:00" />
        </label>
        <label class="filter-item"><span>只看新增聚合值</span>
          <input type="checkbox" v-model="onlyNew" />
        </label>
        <button class="btn" type="submit" data-testid="increment-query">按窗口筛选</button>
        <button class="btn ghost" type="button" @click="resetWindowFilter">重置</button>
      </form>

      <p v-if="incrementMessage" class="increment-message" :class="incrementError ? 'error-text' : ''">{{ incrementMessage }}</p>

      <table class="data-table">
        <thead>
          <tr>
            <th>源版本</th><th>时间窗口</th><th>来源主题</th><th>聚合函数</th>
            <th>新增聚合值</th><th>状态</th><th>结果表版本</th><th>写入批次</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in incrementRows" :key="`${row.metric_id}-${row.source_version}-${row.window_start}`">
            <td>{{ row.source_version }}</td>
            <td>{{ row.window_start }} ~ {{ row.window_end }}</td>
            <td>{{ row.topic_code }}</td>
            <td>{{ row.agg_func }}</td>
            <td>{{ row.agg_value }}</td>
            <td>
              <span :class="row.row_state === '新增' ? 'trend-up' : ''">{{ row.row_state }}</span>
            </td>
            <td>v{{ row.result_table_version }}</td>
            <td>{{ row.batch_id }}</td>
          </tr>
          <tr v-if="!incrementRows.length">
            <td colspan="8" class="empty-state">暂无增量结果，先接入短窗口再执行增量刷新</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/realtime_metrics'
const INCREMENT_ENDPOINT = '/api/metric_increment'
const METRIC_ID = 1 // 实时订单吞吐量 REAL-0001
const summaryMetrics = ref<{label:string;value:number;trend:number}[]>([])
const results = ref<any[]>([]); const chartData = ref<{label:string;value:number}[]>([])
const loading = ref(false)
const params = ref<Record<string,string>>({})
const paramFields = ["指标编号", "指标名称", "聚合窗口", "聚合函数", "过滤条件", "分组维度", "输出表名", "指标版本"].filter((f: string) => !['编号','名称','版本'].includes(f)).slice(0, 4)
const options: Record<string,string[]> = { '统计粒度': ['按日','按周','按月'], '分组字段': ['全量','按类型','按区域'], '比较方法': ['环比','同比','绝对值'], '时间范围': ['最近7天','最近30天','最近90天'] }
const resultColumns = computed(() => ["指标编号", "指标名称", "聚合窗口", "聚合函数", "过滤条件", "分组维度", "输出表名", "指标版本"].slice(0, 5))
function barHeight(v: number) { const max = Math.max(...chartData.value.map(d => d.value), 1); return Math.max(10, (v / max) * 140) }
async function fetchSummary() { try { const res = await request(`${ENDPOINT}/summary`); summaryMetrics.value = (await res.json()).metrics || [] } catch(e) {} }
async function runAnalysis() {
  loading.value = true
  try { const res = await request(`${ENDPOINT}/run`, { method: 'POST', body: JSON.stringify({ values: params.value }) }); const data = await res.json(); results.value = data.items || []; chartData.value = (data.items || []).map((r: any) => ({ label: Object.values(r)[0] as string, value: Object.values(r).filter(v => typeof v === 'number')[0] as number || 0 })).slice(0, 6) } catch(e) { results.value = [] }
  loading.value = false
}

// ── 指标增量结果表 ──────────────────────────────
type IncrementRow = {
  metric_id: number
  source_version: number
  window_start: string
  window_end: string
  topic_code: string
  agg_func: string
  agg_value: number
  row_state: string
  result_table_version: number
  batch_id: string
}
type Cursor = { consumed_version: number; table_version: number; last_batch_id: string | null }
const incrementRows = ref<IncrementRow[]>([])
const cursor = ref<Partial<Cursor>>({})
const busy = ref(false)
const windowFilter = ref({ start: '', end: '' })
const onlyNew = ref(false)
const incrementMessage = ref('')
const incrementError = ref(false)
const newRowCount = computed(() => incrementRows.value.filter(r => r.row_state === '新增').length)

function setMessage(text: string, isError = false) {
  incrementMessage.value = text
  incrementError.value = isError
}

async function loadCursor() {
  try {
    const res = await request(`${INCREMENT_ENDPOINT}/metrics/${METRIC_ID}/cursor`)
    const data = await res.json()
    if (data.ok) cursor.value = data.cursor
  } catch { /* 保留默认游标 */ }
}

async function loadIncrementRows() {
  const query = new URLSearchParams()
  if (windowFilter.value.start) query.set('window_start', windowFilter.value.start)
  if (windowFilter.value.end) query.set('window_end', windowFilter.value.end)
  if (onlyNew.value) query.set('only_new', 'true')
  const res = await request(`${INCREMENT_ENDPOINT}/results?metric_id=${METRIC_ID}&${query.toString()}`)
  const data = await res.json()
  incrementRows.value = data.items ?? []
}

function resetWindowFilter() {
  windowFilter.value = { start: '', end: '' }
  onlyNew.value = false
  void loadIncrementRows()
}

async function postAction(path: string, body?: unknown) {
  const res = await request(`${INCREMENT_ENDPOINT}${path}`, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  return res.json()
}

async function ingestVersions(count: number) {
  busy.value = true
  try {
    const data = await postAction(`/metrics/${METRIC_ID}/ingest`, { count })
    setMessage(`已接入 ${data.ingested} 个短窗口源版本（版本号连续，未重扫历史）`)
    await Promise.all([loadCursor(), loadIncrementRows()])
  } catch (e) {
    setMessage(e instanceof Error ? e.message : '接入失败', true)
  } finally { busy.value = false }
}

async function refreshIncrement() {
  busy.value = true
  try {
    const data = await postAction(`/metrics/${METRIC_ID}/refresh`)
    const skipped = (data.skipped ?? []).length
    setMessage(`${data.message}；新写入 ${data.written.length} 个窗口${skipped ? `，去重跳过 ${skipped} 个` : ''}`)
    await Promise.all([loadCursor(), loadIncrementRows()])
  } catch (e) {
    setMessage(e instanceof Error ? e.message : '增量刷新失败', true)
  } finally { busy.value = false }
}

async function simulateInterrupt() {
  busy.value = true
  try {
    const data = await postAction(`/metrics/${METRIC_ID}/interrupt`, { fail_after: 2 })
    setMessage(`已模拟写入中断：${data.written.length} 个窗口落表，但已消费版本停在 ${data.consumed_after} 未推进`)
    await Promise.all([loadCursor(), loadIncrementRows()])
  } catch (e) {
    setMessage(e instanceof Error ? e.message : '中断演练失败', true)
  } finally { busy.value = false }
}

async function recoverIncrement() {
  busy.value = true
  try {
    const data = await postAction(`/metrics/${METRIC_ID}/recover`)
    const skipped = (data.skipped ?? []).length
    setMessage(`中断恢复完成：补齐 ${data.written.length} 个窗口，去重跳过 ${skipped} 个已写窗口，已消费版本推进到 ${data.consumed_after}`)
    await Promise.all([loadCursor(), loadIncrementRows()])
  } catch (e) {
    setMessage(e instanceof Error ? e.message : '中断恢复失败', true)
  } finally { busy.value = false }
}

async function deliverToLifecycle() {
  busy.value = true
  try {
    const data = await postAction(`/metrics/${METRIC_ID}/deliver`)
    setMessage(data.duplicated ? data.message : `${data.message}（结果表 v${data.delivery.result_table_version}，已消费版本 ${data.delivery.consumed_version}）`)
  } catch (e) {
    setMessage(e instanceof Error ? e.message : '交付失败', true)
  } finally { busy.value = false }
}

onMounted(() => {
  fetchSummary()
  loadCursor()
  loadIncrementRows()
})
</script>
<style scoped>
.increment-panel {
  margin-top: 24px;
  padding: 16px;
  border: 1px solid var(--border, #e4e7ec);
  border-radius: 8px;
  background: var(--surface, #fff);
}
.increment-actions { margin: 12px 0; flex-wrap: wrap; }
.increment-message { margin: 8px 0; font-size: 13px; }
</style>
