import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import { Badge, Button, Card, EmptyState, StatusPill, Toggle } from '../../components/ui'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { airflowApi } from '../../services/endpoints'

const PAGE_SIZE = 50

function SlaCell({ dag, canEdit, onSave }) {
  const [value, setValue] = useState(dag.sla_minutes ?? '')
  const [saving, setSaving] = useState(false)
  if (!canEdit) return <span className="small">{dag.sla_minutes ? `${dag.sla_minutes} min` : '—'}</span>

  async function commit() {
    const next = value === '' ? null : Number(value)
    if (next === (dag.sla_minutes ?? null)) return
    if (next !== null && (!Number.isInteger(next) || next < 5 || next > 10080)) {
      setValue(dag.sla_minutes ?? '')
      onSave(null, 'SLA must be a whole number of minutes between 5 and 10080, or empty.')
      return
    }
    setSaving(true)
    await onSave(next)
    setSaving(false)
  }

  return (
    <input
      className="sla-input"
      type="number"
      min={5}
      max={10080}
      placeholder="none"
      value={value}
      disabled={saving}
      onChange={(e) => setValue(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => e.key === 'Enter' && e.currentTarget.blur()}
      aria-label={`SLA minutes for ${dag.dag_id}`}
      title="Freshness SLA: alert when no successful run finished within this many minutes"
    />
  )
}

export default function MonitoredDags() {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [params, setParams] = useSearchParams()
  const [connections, setConnections] = useState(null)
  const [page, setPage] = useState(null)
  const [offset, setOffset] = useState(0)
  const [search, setSearch] = useState('')
  const [monitoredOnly, setMonitoredOnly] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [pending, setPending] = useState(() => new Set())

  const canOperate = hasRole('ADMIN', 'OPERATOR')
  const connectionId = params.get('connection')
  const connection = connections?.find((c) => c.id === connectionId)

  useEffect(() => {
    airflowApi
      .listConnections()
      .then(({ items }) => {
        setConnections(items)
        if (!params.get('connection') && items.length) {
          const preferred = items.find((c) => c.is_default) ?? items[0]
          setParams({ connection: preferred.id }, { replace: true })
        }
      })
      .catch((err) => toast.error(err.message))
    // eslint-disable-next-line react-hooks/exhaustive-deps -- load connections once on mount
  }, [])

  const loadDags = useCallback(async () => {
    if (!connectionId) return
    try {
      setPage(
        await airflowApi.listDags(connectionId, {
          limit: PAGE_SIZE,
          offset,
          search: search.trim(),
          monitored: monitoredOnly ? true : undefined,
        }),
      )
    } catch (err) {
      toast.error(err.message)
    }
  }, [connectionId, offset, search, monitoredOnly, toast])

  useEffect(() => {
    const timer = setTimeout(loadDags, 200) // debounce search typing
    return () => clearTimeout(timer)
  }, [loadDags])

  async function onSync() {
    setSyncing(true)
    try {
      const r = await airflowApi.syncDags(connectionId)
      toast.success(`Synced ${r.total} DAGs: ${r.created} new, ${r.updated} updated, ${r.missing} missing`)
      await loadDags()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setSyncing(false)
    }
  }

  async function onToggle(dag, value) {
    setPending((s) => new Set(s).add(dag.id))
    try {
      const updated = await airflowApi.setMonitored(dag.id, value)
      setPage((p) => ({ ...p, items: p.items.map((d) => (d.id === updated.id ? updated : d)) }))
    } catch (err) {
      toast.error(err.message)
    } finally {
      setPending((s) => {
        const next = new Set(s)
        next.delete(dag.id)
        return next
      })
    }
  }

  async function onSaveSla(dag, minutes, validationError) {
    if (validationError) return toast.error(validationError)
    try {
      const updated = await airflowApi.setSla(dag.id, minutes)
      setPage((p) => ({ ...p, items: p.items.map((d) => (d.id === updated.id ? updated : d)) }))
      toast.success(minutes ? `SLA for ${dag.dag_id}: ${minutes} min` : `SLA removed for ${dag.dag_id}`)
    } catch (err) {
      toast.error(err.message)
    }
  }

  if (connections && connections.length === 0) {
    return (
      <div className="page">
        <h1>Monitored DAGs</h1>
        <Card>
          <EmptyState title="No Airflow connections">
            <Link to="/settings/connections">Register a connection</Link> first.
          </EmptyState>
        </Card>
      </div>
    )
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Monitored DAGs</h1>
          <p className="muted">Monitored DAGs are the input for failure and SLA detection.</p>
        </div>
        {canOperate && connectionId && (
          <Button variant="primary" onClick={onSync} loading={syncing} disabled={connection && !connection.is_active}>
            Sync DAGs
          </Button>
        )}
      </div>

      <Card>
        <div className="toolbar">
          <label className="inline-field">
            <span>Connection</span>
            <select
              value={connectionId ?? ''}
              onChange={(e) => {
                setOffset(0)
                setParams({ connection: e.target.value })
              }}
            >
              {connections?.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                  {c.is_default ? ' (default)' : ''}
                </option>
              ))}
            </select>
          </label>
          {connection && <StatusPill status={connection.last_health_status} />}
          <span className="spacer" />
          <input
            type="search"
            placeholder="Search DAG id or description"
            value={search}
            onChange={(e) => {
              setOffset(0)
              setSearch(e.target.value)
            }}
            aria-label="Search DAGs"
          />
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={monitoredOnly}
              onChange={(e) => {
                setOffset(0)
                setMonitoredOnly(e.target.checked)
              }}
            />
            Monitored only
          </label>
        </div>

        {!page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title={search || monitoredOnly ? 'No DAGs match the filters' : 'No DAGs synced yet'}>
            {!search && !monitoredOnly && (canOperate ? 'Use “Sync DAGs” to import them from Airflow.' : 'An operator needs to sync DAGs.')}
          </EmptyState>
        ) : (
          <>
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Monitor</th>
                    <th>DAG</th>
                    <th>Schedule</th>
                    <th title="Freshness SLA in minutes">SLA (min)</th>
                    <th>State</th>
                    <th>Tags</th>
                    <th>Synced</th>
                  </tr>
                </thead>
                <tbody>
                  {page.items.map((d) => (
                    <tr key={d.id} className={d.is_present ? '' : 'row-muted'}>
                      <td>
                        <Toggle
                          checked={d.is_monitored}
                          disabled={!canOperate || pending.has(d.id)}
                          onChange={(v) => onToggle(d, v)}
                          label={`Monitor ${d.dag_id}`}
                        />
                      </td>
                      <td>
                        <div className="cell-title mono">{d.dag_id}</div>
                        {d.description && <div className="muted small clamp">{d.description}</div>}
                      </td>
                      <td className="mono small">{d.schedule_summary ?? '—'}</td>
                      <td>
                        <SlaCell
                          key={`${d.id}:${d.sla_minutes}`}
                          dag={d}
                          canEdit={canOperate}
                          onSave={(minutes, error) => onSaveSla(d, minutes, error)}
                        />
                      </td>
                      <td>
                        {!d.is_present ? (
                          <Badge tone="danger" title="Not returned by Airflow on the last sync">missing</Badge>
                        ) : d.is_paused ? (
                          <Badge tone="warning">paused</Badge>
                        ) : (
                          <Badge tone="success">active</Badge>
                        )}
                      </td>
                      <td>
                        <div className="tag-list">
                          {d.tags.map((t) => (
                            <Badge key={t}>{t}</Badge>
                          ))}
                        </div>
                      </td>
                      <td className="small">{formatRelative(d.last_synced_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="pager">
              <span className="muted small">
                {offset + 1}–{Math.min(offset + PAGE_SIZE, page.total)} of {page.total}
              </span>
              <Button size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}>
                Previous
              </Button>
              <Button size="sm" disabled={offset + PAGE_SIZE >= page.total} onClick={() => setOffset(offset + PAGE_SIZE)}>
                Next
              </Button>
            </div>
          </>
        )}
      </Card>
    </div>
  )
}
