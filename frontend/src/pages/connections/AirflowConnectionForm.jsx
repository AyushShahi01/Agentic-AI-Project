import { useState } from 'react'
import { Alert, Button, Field, Modal, StatusPill } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { airflowApi } from '../../services/endpoints'

const EMPTY = {
  name: '',
  environment: 'DEV',
  kind: 'LIVE',
  base_url: 'http://localhost:8080',
  auth_type: 'BASIC',
  username: '',
  secret: '',
  is_active: true,
  is_default: false,
}

export function TestResult({ result }) {
  if (!result) return null
  return (
    <div className={`test-result ${result.ok ? 'test-ok' : 'test-fail'}`}>
      <StatusPill status={result.status} />
      <p>{result.message}</p>
      <dl className="kv">
        {result.latency_ms != null && (
          <>
            <dt>Latency</dt>
            <dd>{result.latency_ms} ms</dd>
          </>
        )}
        {result.airflow_version && (
          <>
            <dt>Airflow</dt>
            <dd>
              {result.airflow_version} (API {result.api_version})
            </dd>
          </>
        )}
      </dl>
    </div>
  )
}

/**
 * Create/edit modal. `connection` is null for create.
 * `allowMock` is false in production (the backend rejects mock there too).
 */
export default function ConnectionForm({ connection, allowMock, onClose, onSaved }) {
  const toast = useToast()
  const editing = Boolean(connection)
  const [form, setForm] = useState(() =>
    editing ? { ...EMPTY, ...connection, username: connection.username ?? '', secret: '' } : EMPTY,
  )
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState(null)

  const set = (key) => (e) => {
    const value = e.target.type === 'checkbox' ? e.target.checked : e.target.value
    setForm((f) => ({ ...f, [key]: value }))
    setTestResult(null)
  }

  const isLive = form.kind === 'LIVE'
  const needsSecret = isLive && form.auth_type !== 'NONE'
  const canTestUnsaved = !needsSecret || Boolean(form.secret)

  function targetPayload() {
    return {
      kind: form.kind,
      base_url: form.base_url.trim(),
      auth_type: isLive ? form.auth_type : 'NONE',
      username: isLive && form.auth_type === 'BASIC' ? form.username.trim() : null,
      secret: needsSecret && form.secret ? form.secret : null,
    }
  }

  async function onTest() {
    setError(null)
    setTesting(true)
    try {
      setTestResult(await airflowApi.testUnsaved(targetPayload()))
    } catch (err) {
      setError(err.message)
    } finally {
      setTesting(false)
    }
  }

  async function onSubmit(event) {
    event.preventDefault()
    setError(null)
    if (!form.name.trim()) return setError('Name is required.')
    if (!/^https?:\/\//.test(form.base_url.trim())) return setError('Base URL must start with http:// or https://')
    if (isLive && form.auth_type === 'BASIC' && !form.username.trim()) return setError('Username is required for Basic auth.')
    if (!editing && needsSecret && !form.secret) return setError('Password or token is required.')

    const target = targetPayload()
    const payload = {
      name: form.name.trim(),
      environment: form.environment,
      is_active: form.is_active,
      is_default: form.is_default,
      ...target,
    }
    if (editing && !form.secret) delete payload.secret // keep stored secret

    setSaving(true)
    try {
      const saved = editing
        ? await airflowApi.updateConnection(connection.id, payload)
        : await airflowApi.createConnection(payload)
      toast.success(editing ? `Updated “${saved.name}”` : `Created “${saved.name}”`)
      onSaved(saved)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      title={editing ? `Edit ${connection.name}` : 'New Airflow connection'}
      onClose={onClose}
      wide
      footer={
        <>
          <Button
            onClick={onTest}
            loading={testing}
            disabled={!canTestUnsaved}
            title={canTestUnsaved ? '' : 'Enter the password/token to test before saving'}
          >
            Test connection
          </Button>
          <span className="spacer" />
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="connection-form" loading={saving}>
            {editing ? 'Save changes' : 'Create connection'}
          </Button>
        </>
      }
    >
      <form id="connection-form" className="form-grid" onSubmit={onSubmit} noValidate>
        {error && <Alert tone="danger">{error}</Alert>}
        <Field label="Name">
          {(id) => <input id={id} value={form.name} onChange={set('name')} maxLength={100} autoFocus />}
        </Field>
        <div className="form-row">
          <Field label="Environment">
            {(id) => (
              <select id={id} value={form.environment} onChange={set('environment')}>
                <option value="DEV">Development</option>
                <option value="STAGING">Staging</option>
                <option value="PROD">Production</option>
              </select>
            )}
          </Field>
          <Field label="Type" hint={form.kind === 'MOCK' ? 'Simulated Airflow for local testing' : undefined}>
            {(id) => (
              <select id={id} value={form.kind} onChange={set('kind')}>
                <option value="LIVE">Live Airflow</option>
                {(allowMock || form.kind === 'MOCK') && <option value="MOCK">Mock (simulated)</option>}
              </select>
            )}
          </Field>
        </div>
        <Field
          label="Base URL"
          hint={
            isLive
              ? 'Airflow webserver root, e.g. http://airflow:8080 (the API version is detected).'
              : 'Any URL. Add ?simulate=UNAUTHORIZED (or another status) to simulate failures.'
          }
        >
          {(id) => <input id={id} value={form.base_url} onChange={set('base_url')} placeholder="http://localhost:8080" />}
        </Field>
        {isLive && (
          <>
            <Field label="Authentication">
              {(id) => (
                <select id={id} value={form.auth_type} onChange={set('auth_type')}>
                  <option value="BASIC">Username &amp; password</option>
                  <option value="TOKEN">Bearer token</option>
                  <option value="NONE">None</option>
                </select>
              )}
            </Field>
            <div className="form-row">
              {form.auth_type === 'BASIC' && (
                <Field label="Username">
                  {(id) => <input id={id} value={form.username} onChange={set('username')} autoComplete="off" />}
                </Field>
              )}
              {form.auth_type !== 'NONE' && (
                <Field
                  label={form.auth_type === 'TOKEN' ? 'Token' : 'Password'}
                  hint={editing && connection.has_secret ? 'Leave blank to keep the current value.' : undefined}
                >
                  {(id) => (
                    <input
                      id={id}
                      type="password"
                      value={form.secret}
                      onChange={set('secret')}
                      autoComplete="new-password"
                      placeholder={editing && connection.has_secret ? '••••••••' : ''}
                    />
                  )}
                </Field>
              )}
            </div>
          </>
        )}
        <div className="checkbox-row">
          <label>
            <input type="checkbox" checked={form.is_active} onChange={set('is_active')} /> Active
          </label>
          <label>
            <input type="checkbox" checked={form.is_default} onChange={set('is_default')} /> Default connection
          </label>
        </div>
        <TestResult result={testResult} />
      </form>
    </Modal>
  )
}
