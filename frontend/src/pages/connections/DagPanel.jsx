import { useCallback, useEffect, useState } from 'react'
import { Badge, Button, EmptyState, Toggle } from '../../components/ui'
import { connectionState } from '../../connectionState'
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

/**
 * The DAGs of one Airflow connection, shown inside the connection catalog. Monitored DAGs are
 * the input for failure and SLA detection. `onConnectionChanged` reloads the catalog after a
 * sync or retry changed the connection's health.
 */
export default function DagPanel({ connection, onConnectionChanged }) {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [page, setPage] = useState(null)
  const [offset, setOffset] = useState(0)
  const [search, setSearch] = useState('')
  const [monitoredOnly, setMonitoredOnly] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [pending, setPending] = useState(() => new Set())

  const canOperate = hasRole('ADMIN', 'OPERATOR')
  const state = connectionState(connection, { everConnected: page?.total > 0 })

  const loadDags = useCallback(async () => {
    try {
      setPage(
        await airflowApi.listDags(connection.id, {
          limit: PAGE_SIZE,
          offset,
          search: search.trim(),
          monitored: monitoredOnly ? true : undefined,
        }),
      )
    } catch (err) {
      toast.error(err.message)
    }
  }, [connection.id, offset, search, monitoredOnly, toast])

  useEffect(() => {
    const timer = setTimeout(loadDags, 200) // debounce search typing
    return () => clearTimeout(timer)
  }, [loadDags])

  async function onSync() {
    setSyncing(true)
    try {
      const r = await airflowApi.syncDags(connection.id)
      toast.success(`Synced ${r.total} DAGs: ${r.created} new, ${r.updated} updated, ${r.missing} missing`)
      await loadDags()
    } catch (err) {
      toast.error(err.message)
    } finally {
      await onConnectionChanged()
      setSyncing(false)
    }
  }

  async function onRetry() {
    setSyncing(true)
    try {
      const updated = await airflowApi.refreshConnection(connection.id)
      if (updated.is_live) toast.success(`${updated.name} is connected`)
      else toast.error(`${updated.name}: ${updated.last_health_message ?? 'still not reachable'}`)
      await Promise.all([onConnectionChanged(), loadDags()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setSyncing(false)
    }
  }

  async function updateDag(dag, changes) {
    setPending((s) => new Set(s).add(dag.id))
    try {
      const updated = await airflowApi.updateDag(dag.id, changes)
      setPage((p) => ({ ...p, items: p.items.map((d) => (d.id === updated.id ? updated : d)) }))
      return updated
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
    if (await updateDag(dag, { sla_minutes: minutes })) {
      toast.success(minutes ? `SLA for ${dag.dag_id}: ${minutes} min` : `SLA removed for ${dag.dag_id}`)
    }
  }

  if (!state.live) {
    return (
      <EmptyState title={state.label}>
        <p>{state.hint}</p>
        {state.message && <p className="muted small">{state.message}</p>}
        <p className="muted small">Last checked {formatRelative(connection.last_checked_at)}</p>
        {canOperate && connection.is_active && (
          <Button size="sm" onClick={onRetry} loading={syncing}>
            Retry now
          </Button>
        )}
      </EmptyState>
    )
  }

  return (
    <div className="dag-panel">
      <div className="toolbar">
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
        <span className="spacer" />
        {canOperate && (
          <Button size="sm" onClick={onSync} loading={syncing}>
            Sync DAGs
          </Button>
        )}
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
                  <th title="Open an incident when a run fails">Alert on failure</th>
                  <th title="Freshness SLA in minutes">SLA (min)</th>
                  <th>State</th>
                  <th>Schedule</th>
                  <th>Tags</th>
                </tr>
              </thead>
              <tbody>
                {page.items.map((d) => (
                  <tr key={d.id} className={d.is_present ? '' : 'row-muted'}>
                    <td>
                      <Toggle
                        checked={d.is_monitored}
                        disabled={!canOperate || pending.has(d.id)}
                        onChange={(v) => updateDag(d, { is_monitored: v })}
                        label={`Monitor ${d.dag_id}`}
                      />
                    </td>
                    <td>
                      <div className="cell-title mono">{d.dag_id}</div>
                      {d.description && <div className="muted small clamp">{d.description}</div>}
                    </td>
                    <td title={d.is_monitored ? '' : 'Turn on monitoring first'}>
                      <Toggle
                        checked={d.is_monitored && d.detect_failures}
                        disabled={!canOperate || !d.is_monitored || pending.has(d.id)}
                        onChange={(v) => updateDag(d, { detect_failures: v })}
                        label={`Alert on failures of ${d.dag_id}`}
                      />
                    </td>
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
                    <td className="mono small">{d.schedule_summary ?? '—'}</td>
                    <td>
                      <div className="tag-list">
                        {d.tags.map((t) => (
                          <Badge key={t}>{t}</Badge>
                        ))}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {page.total > PAGE_SIZE && (
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
          )}
        </>
      )}
    </div>
  )
}
