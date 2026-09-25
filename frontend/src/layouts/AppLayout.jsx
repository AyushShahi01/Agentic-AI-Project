import { useCallback, useEffect, useState } from 'react'
import { NavLink, Outlet } from 'react-router'
import { StatusPill } from '../components/ui'
import { useAuth } from '../context/AuthContext'
import { automationApi, healthApi, incidentsApi } from '../services/endpoints'

const HEALTH_POLL_MS = 30_000

function HealthIndicator({ health, error }) {
  if (error) return <StatusPill status="error" label="API unreachable" title={error} />
  if (!health) return <StatusPill status="UNKNOWN" label="Checking…" />
  const db = health.database.status
  const failing = health.airflow.filter((a) => !['HEALTHY', 'UNKNOWN'].includes(a.status))
  const title = [
    `Database: ${db}`,
    ...health.airflow.map((a) => `Airflow “${a.name}”: ${a.status}`),
  ].join('\n')
  const label =
    health.status === 'ok'
      ? 'All systems OK'
      : db !== 'ok'
        ? 'Database error'
        : `${failing.length} Airflow issue${failing.length === 1 ? '' : 's'}`
  return <StatusPill status={health.status} label={label} title={title} />
}

export default function AppLayout() {
  const { user, logout, hasRole } = useAuth()
  const [health, setHealth] = useState(null)
  const [healthError, setHealthError] = useState(null)
  const [incidentSummary, setIncidentSummary] = useState(null)
  const [automationSummary, setAutomationSummary] = useState(null)

  // Refreshes the header health pill and the sidebar badges together.
  const refreshHealth = useCallback(async () => {
    const [healthResult, summaryResult, automationResult] = await Promise.allSettled([
      healthApi.status(),
      incidentsApi.summary(),
      automationApi.summary(),
    ])
    if (healthResult.status === 'fulfilled') {
      setHealth(healthResult.value)
      setHealthError(null)
    } else {
      setHealthError(healthResult.reason.message)
    }
    if (summaryResult.status === 'fulfilled') setIncidentSummary(summaryResult.value)
    if (automationResult.status === 'fulfilled') setAutomationSummary(automationResult.value)
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    refreshHealth()
    const timer = setInterval(refreshHealth, HEALTH_POLL_MS)
    return () => clearInterval(timer)
  }, [refreshHealth])

  const navClass = ({ isActive }) => `nav-link ${isActive ? 'active' : ''}`

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">◆</span>
          <span>Agentic Ops</span>
        </div>
        <nav>
          <NavLink to="/" end className={navClass}>
            Dashboard
          </NavLink>
          <NavLink to="/incidents" className={navClass}>
            <span>Incidents</span>
            {incidentSummary?.open_total > 0 && (
              <span className="nav-badge" title="Open incidents">
                {incidentSummary.open_total}
              </span>
            )}
          </NavLink>
          <NavLink to="/pipelines" className={navClass}>
            Pipelines
          </NavLink>
          <div className="nav-section">Automation</div>
          <NavLink to="/automation/approvals" className={navClass}>
            <span>Approvals</span>
            {automationSummary?.pending_approvals > 0 && (
              <span className="nav-badge nav-badge-warning" title="Waiting for approval">
                {automationSummary.pending_approvals}
              </span>
            )}
          </NavLink>
          <NavLink to="/automation/runs" className={navClass}>
            Runs
          </NavLink>
          <NavLink to="/automation/workflows" className={navClass}>
            Workflows
          </NavLink>
          <NavLink to="/automation/notifications" className={navClass}>
            <span>Notifications</span>
            {automationSummary?.unread_notifications > 0 && (
              <span className="nav-badge nav-badge-neutral" title="Unread">
                {automationSummary.unread_notifications}
              </span>
            )}
          </NavLink>
          <div className="nav-section">Settings</div>
          <NavLink to="/settings/connections" className={navClass}>
            Airflow Connections
          </NavLink>
          <NavLink to="/settings/dags" className={navClass}>
            Monitored DAGs
          </NavLink>
          {hasRole('ADMIN') && (
            <NavLink to="/settings/users" className={navClass}>
              Users
            </NavLink>
          )}
        </nav>
      </aside>

      <div className="main">
        <header className="topbar">
          <HealthIndicator health={health} error={healthError} />
          <div className="user-menu">
            <div className="user-meta">
              <span className="user-name">{user.full_name}</span>
              <span className="user-role">{user.role.toLowerCase()}</span>
            </div>
            <button type="button" className="btn btn-ghost btn-sm" onClick={logout}>
              Sign out
            </button>
          </div>
        </header>
        <main className="content">
          <Outlet context={{ health, incidentSummary, automationSummary, refreshHealth }} />
        </main>
      </div>
    </div>
  )
}
