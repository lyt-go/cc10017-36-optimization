<template>
  <section class="page" data-module="ttl_policy">
    <header class="page-head">
      <div>
        <h2>数据生命周期管理</h2>
        <p class="page-desc">维护湖表数据的冷热分层保留、归档与过期策略，为数据生命周期提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记生命周期</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出数据生命周期清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无数据生命周期数据，可先登记生命周期</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记生命周期</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <!-- 接入实时指标增量结果表：核对来源主题的可消费保留期 -->
    <div class="retention-panel">
      <div class="page-actions">
        <h3 style="margin:0">指标来源保留期核对</h3>
        <button class="btn" type="button" data-testid="retention-check" @click="loadRetention">核对来源</button>
      </div>
      <p class="page-desc">数据生命周期模块接入实时指标交付的增量结果表，按主题保留时长判断哪些主题仍处于可消费保留期。</p>
      <div class="stat-row">
        <article class="stat-card"><span class="stat-label">来源主题数</span><strong class="stat-value">{{ retention.total_topics ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">可消费</span><strong class="stat-value">{{ retention.consumable_topics ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">已超期</span><strong class="stat-value">{{ retention.expired_topics ?? 0 }}</strong></article>
        <article class="stat-card"><span class="stat-label">待核对</span><strong class="stat-value">{{ retention.pending_topics ?? 0 }}</strong></article>
      </div>
      <table class="data-table" v-if="retentionItems.length">
        <thead>
          <tr><th>来源主题</th><th>主题名称</th><th>保留时长</th><th>水位检查时间</th><th>窗口数</th><th>是否可消费</th><th>核对说明</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in retentionItems" :key="item.topic_code">
            <td>{{ item.topic_code }}</td>
            <td>{{ item.topic_name ?? '—' }}</td>
            <td>{{ item.retention ?? '—' }}</td>
            <td>{{ item.watermark_check_time ?? '—' }}</td>
            <td>{{ item.window_count }}</td>
            <td>
              <span v-if="item.consumable === true" class="trend-up">可消费</span>
              <span v-else-if="item.consumable === false" class="trend-down">已超期</span>
              <span v-else>待核对</span>
            </td>
            <td>{{ item.reason }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="retentionMessage" class="error-text">{{ retentionMessage }}</p>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条数据生命周期记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/ttl_policy'
const columns = ["策略编号", "目标表名", "热数据保留", "温数据窗口", "冷归档策略", "过期动作", "分区粒度", "恢复超时"]
const actions = ["提交校验", "标记生效", "停用策略"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["生命周期策略总数", "待校验策略", "已生效策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'ttl_policy.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('数据生命周期动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据生命周期操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('生命周期列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据生命周期列表读取失败'
  }
}

// ── 指标增量结果表来源核对 ──────────────────────────────
type RetentionItem = {
  topic_code: string
  topic_name: string | null
  retention: string | null
  watermark_check_time: string | null
  window_count: number
  consumable: boolean | null
  reason: string
}
const retentionItems = ref<RetentionItem[]>([])
const retention = ref<Record<string, number>>({})
const retentionMessage = ref('')

async function loadRetention() {
  retentionMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/retention/metrics`)
    if (!response.ok) throw new Error('指标来源保留期核对失败')
    const payload = await response.json()
    retentionItems.value = payload.items ?? []
    retention.value = {
      total_topics: payload.total_topics ?? 0,
      consumable_topics: payload.consumable_topics ?? 0,
      expired_topics: payload.expired_topics ?? 0,
      pending_topics: payload.pending_topics ?? 0,
    }
  } catch (error) {
    retentionMessage.value = error instanceof Error ? error.message : '指标来源保留期核对失败'
  }
}

onMounted(() => {
  reload()
  loadRetention()
})
</script>
<style scoped>
.retention-panel {
  margin-top: 24px;
  padding: 16px;
  border: 1px solid var(--border, #e4e7ec);
  border-radius: 8px;
  background: var(--surface, #fff);
}
.retention-panel .stat-row { margin: 12px 0; }
</style>
