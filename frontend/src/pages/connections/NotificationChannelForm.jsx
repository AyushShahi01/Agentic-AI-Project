import { useState } from 'react'
import { Alert, Button, Field, Modal } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { channelsApi } from '../../services/endpoints'
import { CHANNEL_KINDS } from './engines'

const EMPTY = {
  name: '',
  kind: 'SLACK',
  secret: '',
  is_active: true,
  smtp_host: '',
  smtp_port: '',
  security: 'starttls',
  username: '',
  from_address: '',
  default_recipients: '',
}

function list(text) {
  return text
    .split(/[,\s;]+/)
    .map((v) => v.trim())
    .filter(Boolean)
}

/** Create/edit modal for a notification channel. `connection` is null for create. */
export default function NotificationChannelForm({ connection, onClose, onSaved }) {
  const toast = useToast()
  const editing = Boolean(connection)
  const [form, setForm] = useState(() => {
    if (!editing) return EMPTY
    const s = connection.settings ?? {}
    return {
      ...EMPTY,
      name: connection.name,
      kind: connection.kind,
      is_active: connection.is_active,
      smtp_host: s.smtp_host ?? '',
      smtp_port: s.smtp_port ? String(s.smtp_port) : '',
      security: s.security ?? 'starttls',
      username: s.username ?? '',
      from_address: s.from_address ?? '',
      default_recipients: (s.default_recipients ?? []).join(', '),
    }
  })
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)

  const set = (key) => (e) => {
    const value = e.target.type === 'checkbox' ? e.target.checked : e.target.value
    setForm((f) => ({ ...f, [key]: value }))
  }
  const isEmail = form.kind === 'EMAIL'

  async function onSubmit(event) {
    event.preventDefault()
    setError(null)
    if (!form.name.trim()) return setError('Name is required.')
    if (isEmail && (!form.smtp_host.trim() || !form.from_address.trim()))
      return setError('SMTP server and From address are required.')
    if (!isEmail && !editing && !form.secret.trim()) return setError('Paste the webhook link.')

    const payload = { name: form.name.trim(), is_active: form.is_active }
    if (!editing) payload.kind = form.kind
    if (isEmail) {
      payload.email = {
        smtp_host: form.smtp_host.trim(),
        smtp_port: form.smtp_port ? Number(form.smtp_port) : null,
        security: form.security,
        username: form.username.trim() || null,
        from_address: form.from_address.trim(),
        default_recipients: list(form.default_recipients),
      }
    }
    if (form.secret) payload.secret = form.secret.trim()

    setSaving(true)
    try {
      const saved = editing
        ? await channelsApi.updateConnection(connection.id, payload)
        : await channelsApi.createConnection(payload)
      toast.success(editing ? `Updated “${saved.name}”` : `Created “${saved.name}”. Use Test to send a test message.`)
      onSaved(saved)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      title={editing ? `Edit ${connection.name}` : 'New notification channel'}
      onClose={onClose}
      wide
      footer={
        <>
          <span className="spacer" />
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="channel-form" loading={saving}>
            {editing ? 'Save changes' : 'Create channel'}
          </Button>
        </>
      }
    >
      <form id="channel-form" className="form-grid" onSubmit={onSubmit} noValidate>
        {error && <Alert tone="danger">{error}</Alert>}
        <div className="form-row">
          <Field label="Name" hint="Shown in workflows, e.g. “Ops Slack”">
            {(id) => <input id={id} value={form.name} onChange={set('name')} maxLength={100} autoFocus />}
          </Field>
          <Field label="Type">
            {(id) => (
              <select id={id} value={form.kind} onChange={set('kind')} disabled={editing}>
                {Object.entries(CHANNEL_KINDS).map(([key, k]) => (
                  <option key={key} value={key}>
                    {k.label}
                  </option>
                ))}
              </select>
            )}
          </Field>
        </div>
        <p className="muted small">{CHANNEL_KINDS[form.kind].hint}</p>

        {isEmail ? (
          <>
            <div className="form-row">
              <Field label="SMTP server" hint="e.g. smtp.gmail.com or smtp.office365.com">
                {(id) => <input id={id} value={form.smtp_host} onChange={set('smtp_host')} />}
              </Field>
              <Field label="Port" hint={form.security === 'ssl' ? 'Default 465' : 'Default 587'}>
                {(id) => <input id={id} type="number" min={1} max={65535} value={form.smtp_port} onChange={set('smtp_port')} />}
              </Field>
              <Field label="Security">
                {(id) => (
                  <select id={id} value={form.security} onChange={set('security')}>
                    <option value="starttls">STARTTLS</option>
                    <option value="ssl">SSL/TLS</option>
                    <option value="none">None</option>
                  </select>
                )}
              </Field>
            </div>
            <div className="form-row">
              <Field label="Username">
                {(id) => <input id={id} value={form.username} onChange={set('username')} autoComplete="off" />}
              </Field>
              <Field
                label="Password"
                hint={editing && connection.has_secret ? 'Leave blank to keep the current value.' : 'Gmail/Outlook: use an app password'}
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
            </div>
            <Field label="From address">
              {(id) => <input id={id} type="email" value={form.from_address} onChange={set('from_address')} />}
            </Field>
            <Field label="Default recipients" hint="Used when a block lists no addresses. Comma separated.">
              {(id) => (
                <input id={id} value={form.default_recipients} onChange={set('default_recipients')} placeholder="oncall@example.com" />
              )}
            </Field>
          </>
        ) : (
          <Field
            label="Webhook link"
            hint={editing && connection.has_secret ? `Current: ${connection.target}. Leave blank to keep it.` : 'Kept encrypted; never shown again.'}
          >
            {(id) => (
              <input
                id={id}
                type="password"
                value={form.secret}
                onChange={set('secret')}
                autoComplete="off"
                placeholder={form.kind === 'SLACK' ? 'https://hooks.slack.com/services/…' : 'https://…'}
              />
            )}
          </Field>
        )}
        <div className="checkbox-row">
          <label>
            <input type="checkbox" checked={form.is_active} onChange={set('is_active')} /> Active
          </label>
        </div>
      </form>
    </Modal>
  )
}
