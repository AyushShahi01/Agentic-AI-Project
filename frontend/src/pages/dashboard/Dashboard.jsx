import { useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { Card, StatusPill } from '../../components/ui'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { airflowApi, detectionApi } from '../../services/endpoints'

const SEVERITIES = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
const SEVERITY_PILL = { CRITICAL: 'error', HIGH: 'error', MEDIUM: 'degraded', LOW: 'UNKNOWN' }

function Step({ done, children }) {
  return (
    <li className={`step ${done ? 'step-done' : ''}`}>
      <span className="step-mark" aria-hidden="true">{done ? '✓' : ''}</span>
      <span>{children}</span>
    </li>
  )
}

export default function Dashboard() {
  const { health, incidentSummary, automationSummary } = useOutletContext()
  const { user } = useAuth()
  const [connections, setConnections] = useState(null)
  const [dagStats, setDagStats] = useState({ synced: 0, monitored: 0 })
  const [error, setError] = useState(null)
  const [detection, setDetection] = useState(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        detectionApi
          .status()
          .then((d) => !cancelled && setDetection(d))
          .catch(() => {})
        const { items } = await airflowApi.listConnections()
        const stats = await Promise.all(
          items.map(async (c) => {
            const [all, monitored] = await Promise.all([
              airflowApi.listDags(c.id, { limit: 1 }),
              airflowApi.listDags(c.id, { limit: 1, monitored: true }),
            ])
            return { synced: all.total, monitored: monitored.total }
          }),
        )
        if (cancelled) return
        setConnections(items)
        setDagStats(
          stats.reduce(
            (acc, s) => ({ synced: acc.synced + s.synced, monitored: acc.monitored + s.monitored }),
            { synced: 0, monitored: 0 },
          ),
        )
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [])

  const tested = connections?.some((c) => c.last_health_status === 'HEALTHY')

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Welcome, {user.full_name.split(' ')[0]}</h1>
          <p className="muted">Platform foundation status</p>
        </div>
      </div>
      {error && <p className="field-error">{error}</p>}

      <div className="stat-grid">
        <Card title="Database">
          {health ? (
            <>
              <StatusPill status={health.database.status} label={health.database.status === 'ok' ? 'Connected' : 'Error'} />
              <p className="muted small">
                {health.database.latency_ms != null ? `${health.database.latency_ms} ms query latency` : health.database.message}
              </p>
            </>
          ) : (
            <p className="muted">Checking…</p>
          )}
        </Card>
        <Card title="Airflow connections">
          <div className="stat-value">{connections?.length ?? '—'}</div>
          <div className="pill-row">
            {health?.airflow.map((a) => (
              <StatusPill key={a.id} status={a.status} label={a.name} title={a.message ?? a.status} />
            ))}
          </div>
        </Card>
        <Card title="DAGs">
          <div className="stat-value">{dagStats.monitored}</div>
          <p className="muted small">monitored of {dagStats.synced} synced</p>
        </Card>
        <Card title="Open incidents" actions={<Link to="/incidents" className="small">View all</Link>}>
          <div className="stat-value">{incidentSummary?.open_total ?? '—'}</div>
          <div className="pill-row">
            {SEVERITIES.filter((s) => incidentSummary?.by_severity[s]).map((s) => (
              <StatusPill
                key={s}
                status={SEVERITY_PILL[s]}
                label={`${incidentSummary.by_severity[s]} ${s.toLowerCase()}`}
              />
            ))}
          </div>
        </Card>
        <Card
          title="Automation"
          actions={
            <Link to="/automation/approvals" className="small">
              Approvals
            </Link>
          }
        >
          <div className="stat-value">{automationSummary?.pending_approvals ?? '—'}</div>
          <p className="muted small">
            {automationSummary
              ? `waiting for approval · ${automationSummary.active_runs} active run(s) · ${automationSummary.enabled_workflows} workflow(s) on`
              : 'Checking…'}
          </p>
        </Card>
        <Card title="Detection">
          {detection ? (
            <>
              <StatusPill
                status={detection.enabled ? (detection.last_error ? 'degraded' : 'ok') : 'UNKNOWN'}
                label={detection.enabled ? `Every ${Math.round(detection.interval_seconds / 60)} min` : 'Disabled'}
                title={detection.last_error ?? ''}
              />
              <p className="muted small">
                {detection.last_cycle
                  ? `Last cycle ${formatRelative(detection.last_cycle.finished_at)}: ${detection.last_cycle.dags} DAG(s) checked`
                  : 'No cycle has run in this server process yet'}
              </p>
            </>
          ) : (
            <p className="muted">Checking…</p>
          )}
        </Card>
      </div>

      <Card title="Getting started">
        <ol className="steps">
          <Step done={Boolean(connections?.length)}>
            <Link to="/settings/connections">Register an Airflow connection</Link>
          </Step>
          <Step done={tested}>Test the connection until it reports Healthy</Step>
          <Step done={dagStats.synced > 0}>
            <Link to="/settings/dags">Sync DAGs</Link> from Airflow
          </Step>
          <Step done={dagStats.monitored > 0}>Mark the DAGs you want monitored</Step>
          <Step done={Boolean(detection?.last_cycle)}>
            Detection has run (automatically, or via <Link to="/incidents">Run detection now</Link>)
          </Step>
          <Step done={automationSummary?.enabled_workflows > 0}>
            <Link to="/automation/workflows">Turn on an automation workflow</Link> (try dry run first)
          </Step>
        </ol>
        {connections && (
          <p className="muted small">
            Last connection check:{' '}
            {formatRelative(
              connections
                .map((c) => c.last_checked_at)
                .filter(Boolean)
                .sort()
                .at(-1),
            )}
          </p>
        )}
      </Card>
    </div>
  )
}
