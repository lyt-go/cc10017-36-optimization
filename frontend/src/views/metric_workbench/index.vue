<template>
  <section class="page" data-module="metric_workbench">
    <header class="page-head">
      <div>
        <h2>指标工作台</h2>
        <p class="page-desc">同步显示实时指标增量结果表：保留已消费版本与新增聚合值，关联记录刷新后保持其写入时的结果表版本，不被后续刷新覆盖。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="workbench-refresh" @click="reload">刷新工作台</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card"><span class="stat-label">已消费版本</span><strong class="stat-value">{{ cursor.consumed_version ?? 0 }}</strong></article>
      <article class="stat-card"><span class="stat-label">结果表当前版本</span><strong class="stat-value">v{{ cursor.table_version ?? 0 }}</strong></article>
      <article class="stat-card"><span class="stat-label">结果表窗口数</span><strong class="stat-value">{{ rows.length }}</strong></article>
      <article class="stat-card"><span class="stat-label">已交付生命周期版本</span><strong class="stat-value">{{ deliveredVersions }}</strong></article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item"><span>窗口起点（起）</span>
        <input v-model="filters.window_start" data-testid="filter-window-start" placeholder="按时间窗口筛选" />
      </label>
      <label class="filter-item"><span>窗口起点（止）</span>
        <input v-model="filters.window_end" placeholder="按时间窗口筛选" />
      </label>
      <button class="btn" type="submit" data-testid="query">筛选</button>
      <button class="btn ghost" type="button" data-testid="reset" @click="resetFilters">重置条件</button>
    </form>

    <p v-if="message" class="increment-message">{{ message }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>指标</th><th>源版本</th><th>时间窗口</th><th>来源主题</th>
          <th>聚合函数</th><th>新增聚合值</th><th>记录状态</th><th>结果表版本</th><th>写入时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="`${row.metric_id}-${row.source_version}-${row.window_start}`"
            :data-testid="`row-${row.source_version}`" :data-table-version="row.result_table_version">
          <td>{{ row.metric_name }}（{{ row.metric_code }}）</td>
          <td>{{ row.source_version }}</td>
          <td>{{ row.window_start }} ~ {{ row.window_end }}</td>
          <td>{{ row.topic_code }}</td>
          <td>{{ row.agg_func }}</td>
          <td>{{ row.agg_value }}</td>
          <td>{{ row.row_state }}</td>
          <!-- 关联记录刷新后保持原版本：直接展示该行写入时归属的结果表版本 -->
          <td>v{{ row.result_table_version }}</td>
          <td>{{ row.written_at }}</td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="9" class="empty-state">工作台暂无增量结果，请先在实时指标页接入短窗口并执行增量刷新</td>
        </tr>
      </tbody>
    </table>

    <h3 style="margin-top:20px">数据生命周期交付记录</h3>
    <table class="data-table">
      <thead>
        <tr><th>交付编号</th><th>来源模块</th><th>目标模块</th><th>结果表</th><th>保留的当前版本</th><th>已消费版本</th><th>交付时间</th><th>状态</th></tr>
      </thead>
      <tbody>
        <tr v-for="d in deliveries" :key="d.delivery_id">
          <td>{{ d.delivery_id }}</td>
          <td>{{ d.source_module }}</td>
          <td>{{ d.target_module }}</td>
          <td>{{ d.result_table }}</td>
          <td>v{{ d.result_table_version }}</td>
          <td>{{ d.consumed_version }}</td>
          <td>{{ d.delivered_at }}</td>
          <td>{{ d.status }}</td>
        </tr>
        <tr v-if="!deliveries.length">
          <td colspan="8" class="empty-state">尚未向数据生命周期交付结果表</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot"><span>共 {{ rows.length }} 条增量窗口记录，版本随写入批次保留</span></footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/metric_increment'
type IncrementRow = {
  metric_id: number
  metric_code: string
  metric_name: string
  source_version: number
  window_start: string
  window_end: string
  topic_code: string
  agg_func: string
  agg_value: number
  row_state: string
  result_table_version: number
  written_at: string
}
type Delivery = {
  delivery_id: string
  source_module: string
  target_module: string
  result_table: string
  result_table_version: number
  consumed_version: number
  delivered_at: string
  status: string
}
const rows = ref<IncrementRow[]>([])
const deliveries = ref<Delivery[]>([])
const cursor = ref<{ consumed_version?: number; table_version?: number }>({})
const filters = ref({ window_start: '', window_end: '' })
const message = ref('')

const deliveredVersions = computed(() =>
  deliveries.value.length ? deliveries.value.map(d => `v${d.result_table_version}`).join('、') : '—')

function resetFilters() {
  filters.value = { window_start: '', window_end: '' }
  void reload()
}

async function reload() {
  message.value = ''
  try {
    const query = new URLSearchParams()
    if (filters.value.window_start) query.set('window_start', filters.value.window_start)
    if (filters.value.window_end) query.set('window_end', filters.value.window_end)
    const [rowsRes, cursorRes, deliveryRes] = await Promise.all([
      request(`${ENDPOINT}/results?${query.toString()}`),
      request(`${ENDPOINT}/metrics/1/cursor`),
      request(`${ENDPOINT}/deliveries`),
    ])
    const rowsData = await rowsRes.json()
    const cursorData = await cursorRes.json()
    const deliveryData = await deliveryRes.json()
    rows.value = rowsData.items ?? []
    if (cursorData.ok) cursor.value = cursorData.cursor
    deliveries.value = deliveryData.items ?? []
  } catch (e) {
    message.value = e instanceof Error ? e.message : '工作台加载失败'
  }
}

onMounted(reload)
</script>
