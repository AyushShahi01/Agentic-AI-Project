// Smoke tests for the canvas pages against the seeded e2e backend (mock Airflow, DEV connection
// "mock-dev" with partner_api_sync + orders_pipeline monitored, retry-transient turned on).
import { expect, test } from '@playwright/test'

const EMAIL = process.env.E2E_ADMIN_EMAIL ?? 'admin@example.com'
const PASSWORD = process.env.E2E_ADMIN_PASSWORD ?? 'e2e-admin-password-123'
const SHOTS = 'test-results/screens'

async function login(page) {
  await page.goto('/login')
  await page.getByLabel('Email').fill(EMAIL)
  await page.getByLabel('Password').fill(PASSWORD)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('link', { name: 'Pipelines' })).toBeVisible()
}

/** Press on one element and release over another (React Flow connections). */
async function dragBetween(page, from, to) {
  const a = await from.boundingBox()
  const b = await to.boundingBox()
  await page.mouse.move(a.x + a.width / 2, a.y + a.height / 2)
  await page.mouse.down()
  await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2, { steps: 15 })
  await page.mouse.up()
}

const node = (page, id) => page.locator(`.react-flow__node[data-id="${id}"]`)

test.beforeEach(async ({ page }) => {
  await login(page)
})

test('workflow editor: build, save, reload, validate', async ({ page }) => {
  await page.goto('/automation/workflows/new')
  await expect(node(page, 'trigger')).toBeVisible()

  // Drag "Tell someone" from the palette onto the canvas.
  const canvas = page.locator('.editor-canvas')
  await page.getByRole('button', { name: 'Tell someone' }).dragTo(canvas, { targetPosition: { x: 520, y: 260 } })
  await expect(node(page, 'notify_1')).toBeVisible()

  // Wire trigger.next -> notify_1.
  await dragBetween(
    page,
    page.locator('.react-flow__handle[data-nodeid="trigger"][data-handleid="next"]'),
    page.locator('.react-flow__handle.target[data-nodeid="notify_1"]'),
  )
  await expect(page.getByTestId('rf__edge-trigger:next')).toBeAttached()

  // Configure the block in the side panel.
  await node(page, 'notify_1').click()
  await page.getByLabel('Display name').fill('Tell the team')
  await expect(node(page, 'notify_1')).toContainText('Tell the team')

  await page.getByLabel('Workflow name').fill('E2E workflow')
  await page.getByRole('button', { name: 'Create workflow' }).click()
  await expect(page).toHaveURL(/\/automation\/workflows\/[0-9a-f-]{36}$/)
  await page.screenshot({ path: `${SHOTS}/workflow-editor.png` })

  // Reload: same blocks, same wiring.
  await page.reload()
  await expect(node(page, 'notify_1')).toContainText('Tell the team')
  await expect(page.getByTestId('rf__edge-trigger:next')).toBeAttached()
  await expect(page.getByLabel('Workflow name')).toHaveValue('E2E workflow')

  // Break the wiring: the server reports the orphaned block and the canvas highlights it.
  await page.getByTestId('rf__edge-trigger:next').click({ force: true })
  await page.keyboard.press('Delete')
  await page.getByRole('button', { name: 'Validate' }).click()
  await expect(page.locator('.editor-problems')).toContainText('not connected to the trigger')
  await expect(node(page, 'notify_1').locator('.block-problem')).toBeVisible()
  await page.screenshot({ path: `${SHOTS}/workflow-editor-problem.png` })
})

test('workflow editor opens a template read from the server', async ({ page }) => {
  await page.goto('/automation/workflows')
  await page
    .locator('.workflow-card', { hasText: 'Auto-retry transient failures' })
    .getByRole('link', { name: 'Edit on canvas' })
    .click()
  await expect(node(page, 'retry')).toBeVisible()
  await expect(node(page, 'approval')).toContainText('Ask a human')
  await expect(page.locator('.react-flow__edge')).toHaveCount(10)
  await page.screenshot({ path: `${SHOTS}/workflow-template.png` })
})

test('pipelines canvas: attach an SLA monitor by drawing', async ({ page }) => {
  await page.goto('/pipelines')
  await expect(page.locator('.pnode-conn')).toContainText('mock-dev')
  await expect(page.locator('.react-flow__node[data-id^="mon:failure:"]')).toHaveCount(2)
  const partner = page.locator('.react-flow__node[data-id^="dag:"]', { hasText: 'partner_api_sync' })
  await expect(partner).toBeVisible()
  await expect(page.locator('.react-flow__node[data-id^="wf:"]')).toContainText('Auto-retry transient failures')

  await page
    .getByRole('button', { name: 'Freshness SLA' })
    .dragTo(page.locator('.editor-canvas'), { targetPosition: { x: 560, y: 60 } })
  const draft = node(page, 'draft:1')
  await expect(draft).toContainText('Connect a DAG to attach')

  await dragBetween(page, partner.locator('.pnode-handle-out'), page.locator('.react-flow__handle.target[data-nodeid="draft:1"]'))
  await expect(page.getByText('Freshness SLA added to partner_api_sync')).toBeVisible()
  await expect(page.locator('.react-flow__node[data-id^="mon:sla:"]')).toContainText('60 min')
  await expect(draft).toHaveCount(0)
  await page.screenshot({ path: `${SHOTS}/pipelines.png` })
})

test('run replay shows the path a real run took', async ({ page }) => {
  await page.goto('/incidents')
  await page.getByRole('button', { name: 'Run detection now' }).click()
  await expect(page.getByText(/Detection checked/)).toBeVisible()

  await page.goto('/automation/runs')
  await page.getByRole('link', { name: 'Auto-retry transient failures' }).first().click()
  const canvas = page.locator('.run-canvas')
  await expect(canvas.locator('.block-run-success').first()).toBeVisible()
  // trigger, classify, filter, approval (auto in DEV), retry -> all done; verify is waiting.
  await expect(canvas.locator('.block-run-success')).toHaveCount(5)
  await expect(canvas.locator('.block-run-waiting')).toHaveCount(1)
  await expect(canvas.locator('.block-dimmed').first()).toBeVisible()
  await page.screenshot({ path: `${SHOTS}/run-replay.png`, fullPage: true })
})
