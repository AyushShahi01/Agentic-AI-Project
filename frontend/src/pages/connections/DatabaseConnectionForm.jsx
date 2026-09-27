import { useState } from 'react'
import { Alert, Button, Field, Modal } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { databaseApi } from '../../services/endpoints'
import { TestResult } from './AirflowConnectionForm'
import { ENGINES } from './engines'

const SSL_MODES = ['', 'disable', 'prefer', 'require', 'verify-ca', 'verify-full']

const EMPTY = {
  name: '',
  environment: 'DEV',
  engine: 'POSTGRESQL',
  host: 'localhost',
  port: '',
  database: '',
  username: '',
  secret: '',
  ssl_mode: '',
  is_active: true,
}

/** Create/edit modal for a database connection. `connection` is null for create. */
export default function DatabaseConnectionForm({ connection, onClose, onSaved }) {
  const toast = useToast()
  const editing = Boolean(connection)
  const [form, setForm] = useState(() =>
    editing
      ? { ...EMPTY, ...connection, port: String(connection.port), ssl_mode: connection.ssl_mode ?? '', secret: '' }
      : EMPTY,
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

  const isPostgres = form.engine === 'POSTGRESQL'
  // Testing a saved connection without retyping the password is done from the catalog.
  const canTestUnsaved = !editing || !connection.has_secret || Boolean(form.secret)

  function validate() {
    if (!form.host.trim()) return 'Host is required.'
    if (form.port && !(Number.isInteger(Number(form.port)) && Number(form.port) >= 1 && Number(form.port) <= 65535))
      return 'Port must be a number between 1 and 65535.'
    if (!form.database.trim()) return 'Database name is required.'
    if (!form.username.trim()) return 'Username is required.'
    return null
  }

  function targetPayload() {
    return {
      engine: form.engine,
      host: form.host.trim(),
      port: form.port ? Number(form.port) : null,
      database: form.database.trim(),
      username: form.username.trim(),
      secret: form.secret || null,
      ssl_mode: isPostgres && form.ssl_mode ? form.ssl_mode : null,
    }
  }

  async function onTest() {
    setError(null)
    const invalid = validate()
    if (invalid) return setError(invalid)
    setTesting(true)
    try {
      setTestResult(await databaseApi.testUnsaved(targetPayload()))
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
    const invalid = validate()
    if (invalid) return setError(invalid)

    const payload = { name: form.name.trim(), environment: form.environment, is_active: form.is_active, ...targetPayload() }
    if (!payload.port) payload.port = ENGINES[form.engine].port
    if (editing && !form.secret) delete payload.secret // keep stored password

    setSaving(true)
    try {
      const saved = editing
        ? await databaseApi.updateConnection(connection.id, payload)
        : await databaseApi.createConnection(payload)
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
      title={editing ? `Edit ${connection.name}` : 'New database connection'}
      onClose={onClose}
      wide
      footer={
        <>
          <Button
            onClick={onTest}
            loading={testing}
            disabled={!canTestUnsaved}
            title={canTestUnsaved ? '' : 'Enter the password to test before saving'}
          >
            Test connection
          </Button>
          <span className="spacer" />
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="db-connection-form" loading={saving}>
            {editing ? 'Save changes' : 'Create connection'}
          </Button>
        </>
      }
    >
      <form id="db-connection-form" className="form-grid" onSubmit={onSubmit} noValidate>
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
          <Field label="Engine">
            {(id) => (
              <select id={id} value={form.engine} onChange={set('engine')}>
                {Object.entries(ENGINES).map(([key, e]) => (
                  <option key={key} value={key}>
                    {e.label}
                  </option>
                ))}
              </select>
            )}
          </Field>
        </div>
        <div className="form-row">
          <Field label="Host">
            {(id) => <input id={id} value={form.host} onChange={set('host')} placeholder="db.example.com" />}
          </Field>
          <Field label="Port" hint={`Default ${ENGINES[form.engine].port}`}>
            {(id) => (
              <input
                id={id}
                type="number"
                min={1}
                max={65535}
                value={form.port}
                onChange={set('port')}
                placeholder={String(ENGINES[form.engine].port)}
              />
            )}
          </Field>
        </div>
        <Field label="Database">
          {(id) => <input id={id} value={form.database} onChange={set('database')} placeholder="analytics" />}
        </Field>
        <div className="form-row">
          <Field label="Username">
            {(id) => <input id={id} value={form.username} onChange={set('username')} autoComplete="off" />}
          </Field>
          <Field label="Password" hint={editing && connection.has_secret ? 'Leave blank to keep the current value.' : undefined}>
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
        </div>
        {isPostgres && (
          <Field label="SSL mode" hint="Leave on driver default unless your server requires TLS.">
            {(id) => (
              <select id={id} value={form.ssl_mode} onChange={set('ssl_mode')}>
                {SSL_MODES.map((m) => (
                  <option key={m} value={m}>
                    {m || 'Driver default'}
                  </option>
                ))}
              </select>
            )}
          </Field>
        )}
        <div className="checkbox-row">
          <label>
            <input type="checkbox" checked={form.is_active} onChange={set('is_active')} /> Active
          </label>
        </div>
        <TestResult result={testResult} />
      </form>
    </Modal>
  )
}
