// Browser smoke tests for the canvas pages. `npm run test:e2e`
// Starts the seeded backend (backend/scripts/e2e_server.py, mock Airflow) and the Vite dev server.
import { defineConfig, devices } from '@playwright/test'

const python =
  process.env.E2E_PYTHON ??
  (process.platform === 'win32' ? '../backend/.venv/Scripts/python.exe' : '../backend/.venv/bin/python')

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  workers: 1, // the specs share one seeded backend
  reporter: [['list']],
  outputDir: 'test-results',
  use: {
    baseURL: 'http://127.0.0.1:5173',
    viewport: { width: 1440, height: 900 },
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } } }],
  webServer: [
    {
      command: `${python} ../backend/scripts/e2e_server.py --port 8000`,
      url: 'http://127.0.0.1:8000/api/v1/health/live',
      reuseExistingServer: false,
      timeout: 90_000,
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 5173 --strictPort',
      url: 'http://127.0.0.1:5173',
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
    },
  ],
})
