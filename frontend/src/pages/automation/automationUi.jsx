import { useState } from 'react'
import { Badge, Button, Field, Modal, Alert } from '../../components/ui'

const RUN_TONES = {
  PENDING: 'neutral',
  RUNNING: 'info',
  WAITING: 'warning',
  COMPLETED: 'success',
  FAILED: 'danger',
  CANCELLED: 'neutral',
}

const RUN_LABELS = { WAITING: 'waiting', COMPLETED: 'completed' }

export function RunStatusBadge({ run }) {
  const waitingFor = run.status === 'WAITING' && run.current_node ? ` · ${run.current_node}` : ''
  return (
    <span className="cell-title">
      <Badge tone={RUN_TONES[run.status]}>
        {(RUN_LABELS[run.status] ?? run.status.toLowerCase()) + waitingFor}
      </Badge>
      {run.dry_run && <Badge tone="info">dry run</Badge>}
    </span>
  )
}

const APPROVAL_TONES = { PENDING: 'warning', APPROVED: 'success', REJECTED: 'danger', EXPIRED: 'neutral' }

export function ApprovalStatusBadge({ status }) {
  return <Badge tone={APPROVAL_TONES[status]}>{status.toLowerCase()}</Badge>
}

const LEVEL_TONES = { INFO: 'info', WARNING: 'warning', CRITICAL: 'danger' }

export function LevelBadge({ level }) {
  return <Badge tone={LEVEL_TONES[level]}>{level.toLowerCase()}</Badge>
}

/** Approve/reject dialog with an optional comment. */
export function DecisionModal({ approval, approve, onClose, onDecide }) {
  const [comment, setComment] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  async function submit(event) {
    event.preventDefault()
    setSaving(true)
    try {
      await onDecide(comment.trim())
    } catch (err) {
      setError(err.message)
      setSaving(false)
    }
  }

  return (
    <Modal
      title={approve ? 'Approve action' : 'Reject action'}
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant={approve ? 'primary' : 'danger'} type="submit" form="decision-form" loading={saving}>
            {approve ? 'Approve' : 'Reject'}
          </Button>
        </>
      }
    >
      <form id="decision-form" className="form-grid" onSubmit={submit}>
        {error && <Alert tone="danger">{error}</Alert>}
        <p>
          <strong>{approval.title}</strong>
        </p>
        {approval.summary && <p className="muted small">{approval.summary}</p>}
        <Field label="Comment (optional)" hint="Recorded on the incident timeline and in the audit log.">
          {(id) => (
            <textarea id={id} rows={3} value={comment} maxLength={2000} onChange={(e) => setComment(e.target.value)} />
          )}
        </Field>
      </form>
    </Modal>
  )
}
