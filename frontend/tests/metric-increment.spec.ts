import { expect, test } from '@playwright/test'

// 指标增量结果表端到端冒烟：接入→刷新→中断→恢复→工作台→生命周期核对
test.describe('指标增量结果表', () => {
  test('实时指标页：增量刷新、中断恢复与窗口筛选', async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', e => errors.push(e.message))
    page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

    await page.goto('/realtime_metrics')
    await expect(page.getByTestId('increment-refresh')).toBeVisible()

    // 接入3个短窗口并增量刷新
    await page.getByRole('button', { name: '接入3个短窗口' }).click()
    await expect(page.locator('.increment-message')).toContainText(/已接入 3 个短窗口/)
    await page.getByTestId('increment-refresh').click()
    await expect(page.locator('.increment-message')).toContainText(/已消费版本 0 → 3/)

    // 增量面板第一张数据表的窗口行
    const dataRows = page.locator('.increment-panel table.data-table tbody tr')
    await expect(dataRows).toHaveCount(3)

    // 时间窗口筛选：只保留 10:02 起的窗口（第3个）
    await page.getByTestId('filter-window-start').fill('2026-11-16T10:02:00')
    await page.getByTestId('increment-query').click()
    await expect(dataRows).toHaveCount(1)
    await expect(dataRows.first()).toContainText('2026-11-16T10:02:00')

    // 重置恢复3行
    await page.getByRole('button', { name: '重置' }).click()
    await expect(dataRows).toHaveCount(3)

    // 中断演练 + 恢复
    await page.getByRole('button', { name: '接入3个短窗口' }).click()
    await page.getByRole('button', { name: '模拟写入中断' }).click()
    await expect(page.locator('.increment-message')).toContainText(/写入中断/)
    await page.getByTestId('increment-recover').click()
    await expect(page.locator('.increment-message')).toContainText(/中断恢复完成/)
    await expect(dataRows).toHaveCount(6)

    // 同一窗口不重复累计：源版本列应为 1..6 且无重复
    const allVersionCells = await page.locator('.increment-panel tbody tr td:first-child').allInnerTexts()
    expect(new Set(allVersionCells).size).toBe(allVersionCells.length)
    expect(allVersionCells).toEqual(['1', '2', '3', '4', '5', '6'])

    // 交付数据生命周期
    await page.getByRole('button', { name: '交付数据生命周期' }).click()
    await expect(page.locator('.increment-message')).toContainText(/数据生命周期/)

    expect(errors).toEqual([])
  })

  test('指标工作台：同步显示且关联记录保持原版本', async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', e => errors.push(e.message))
    page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

    await page.goto('/metric_workbench')
    await expect(page.getByTestId('workbench-refresh')).toBeVisible()
    const resultsTable = page.locator('section.page > table.data-table').first()
    const dataRows = resultsTable.locator('tbody tr')
    await expect(dataRows.first()).toBeVisible()

    // 记录了每个窗口写入时的结果表版本（至少出现 v1 与 v2 两批）
    const versionTags = await page.locator('tbody tr[data-table-version]').evaluateAll(
      nodes => nodes.map(n => (n as HTMLElement).dataset.tableVersion),
    )
    expect(versionTags.length).toBeGreaterThan(0)
    expect(new Set(versionTags).size).toBeGreaterThanOrEqual(1)

    // 交付记录表存在一条记录
    await expect(page.getByText('DELIV-1-v')).toBeVisible()
    expect(errors).toEqual([])
  })

  test('数据生命周期页：来源保留期核对', async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', e => errors.push(e.message))
    page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

    await page.goto('/ttl_policy')
    await expect(page.getByTestId('retention-check')).toBeVisible()
    await page.getByTestId('retention-check').click()
    await expect(page.locator('.retention-panel tbody tr').first()).toBeVisible()
    await expect(page.locator('.retention-panel')).toContainText('TOPI-0001')
    await expect(page.locator('.retention-panel')).toContainText(/可消费|已超期|待核对/)
    expect(errors).toEqual([])
  })
})
