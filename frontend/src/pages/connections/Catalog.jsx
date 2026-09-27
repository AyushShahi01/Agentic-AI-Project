import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { Badge, Button, Card, EmptyState, Modal, StatusPill } from '../../components/ui'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { airflowApi } from '../../services/endpoints'
import ConnectionForm, { TestResult } from './ConnectionForm'

export default function Connections() {
  const { hasRole } = useAuth()
  const { health, refreshHealth } = useOutletContext()
  const toast = useToast()
  const [connections, setConnections] = useState(null)
  const [editing, setEditing] = useState(undefined) // undefined = closed, null = new, object = edit
  const [testingId, setTestingId] = useState(null)
  const [lastTest, setLastTest] = useState(null) // { connection, result }
  const [deleting, setDeleting] = useState(null)

  const isAdmin = hasRole('ADMIN')
  const canOperate = hasRole('ADMIN', 'OPERATOR')
  const allowMock = health?.environment !== 'production'

  const load = useCallback(async () => {
    try {
      setConnections((await airflowApi.listConnections()).items)
    } catch (err) {
      toast.error(err.message)
    }
  }, [toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    load()
  }, [load])

  async function onTest(conn) {
    setTestingId(conn.id)
    try {
      const result = await airflowApi.testSaved(conn.id)
      setLastTest({ connection: conn, result })
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setTestingId(null)
    }
  }

  async function onDelete() {
    try {
      await airflowApi.deleteConnection(deleting.id)
      toast.success(`Deleted “${deleting.name}”`)
      setDeleting(null)
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Airflow connections</h1>
          <p className="muted">Airflow 2.x and 3.x instances this platform monitors.</p>
        </div>
        {isAdmin && (
          <Button variant="primary" onClick={() => setEditing(null)}>
            New connection
          </Button>
        )}
      </div>

      <Card>
        {connections === null ? (
          <p className="muted">Loading…</p>
        ) : connections.length === 0 ? (
          <EmptyState title="No connections yet">
            {isAdmin
              ? 'Create one to start discovering DAGs. Use the Mock type if no Airflow is running locally.'
              : 'Ask an administrator to register an Airflow instance.'}
          </EmptyState>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Status</th>
                  <th>URL</th>
                  <th>Airflow</th>
                  <th>Last checked</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {connections.map((c) => (
                  <tr key={c.id} className={c.is_active ? '' : 'row-muted'}>
                    <td>
                      <div className="cell-title">
                        {c.name}
                        {c.is_default && <Badge tone="info">default</Badge>}
                        {c.kind === 'MOCK' && <Badge tone="neutral">mock</Badge>}
                        {!c.is_active && <Badge tone="neutral">inactive</Badge>}
                      </div>
                      <div className="muted small">{c.environment.toLowerCase()} · {c.auth_type.toLowerCase()} auth</div>
                    </td>
                    <td>
                      <StatusPill status={c.last_health_status} title={c.last_health_message ?? ''} />
                    </td>
                    <td className="mono small">{c.base_url}</td>
                    <td className="small">
                      {c.airflow_version ? `${c.airflow_version} (${c.api_version})` : '—'}
                    </td>
                    <td className="small">
                      {formatRelative(c.last_checked_at)}
                      {c.last_latency_ms != null && <div className="muted">{c.last_latency_ms} ms</div>}
                    </td>
                    <td className="actions">
                      {canOperate && (
                        <Button size="sm" onClick={() => onTest(c)} loading={testingId === c.id}>
                          Test
                        </Button>
                      )}
                      <Link className="btn btn-sm btn-ghost" to={`/settings/dags?connection=${c.id}`}>
                        DAGs
                      </Link>
                      {isAdmin && (
                        <>
                          <Button size="sm" variant="ghost" onClick={() => setEditing(c)}>
                            Edit
                          </Button>
                          <Button size="sm" variant="ghost-danger" onClick={() => setDeleting(c)}>
                            Delete
                          </Button>
                        </>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {lastTest && (
        <Card
          title={`Test result — ${lastTest.connection.name}`}
          actions={
            <Button size="sm" variant="ghost" onClick={() => setLastTest(null)}>
              Dismiss
            </Button>
          }
        >
          <TestResult result={lastTest.result} />
        </Card>
      )}

      {editing !== undefined && (
        <ConnectionForm
          connection={editing}
          allowMock={allowMock}
          onClose={() => setEditing(undefined)}
          onSaved={async () => {
            setEditing(undefined)
            await Promise.all([load(), refreshHealth()])
          }}
        />
      )}

      {deleting && (
        <Modal
          title="Delete connection?"
          onClose={() => setDeleting(null)}
          footer={
            <>
              <Button onClick={() => setDeleting(null)}>Cancel</Button>
              <Button variant="danger" onClick={onDelete}>
                Delete
              </Button>
            </>
          }
        >
          <p>
            This removes <strong>{deleting.name}</strong> and all of its synced and monitored DAGs.
            The action is recorded in the audit log.
          </p>
        </Modal>
      )}
    </div>
  )
}
