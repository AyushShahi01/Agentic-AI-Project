import { useState } from 'react'
import { Alert, Badge, Button, Card, Field, Modal } from '../ui'

// Mirrors backend FailureCategory / CATEGORY_LABELS (app/diagnosis/log_classifier.py).
const CATEGORIES = [
  ['AUTH', 'Login or permission problem'],
  ['DATA_INTEGRITY', 'Bad data (constraint violation)'],
  ['SCHEMA', 'Table or column changed'],
  ['CODE_BUG', 'Code error'],
  ['RESOURCE', 'Out of memory or disk'],
  ['TIMEOUT', 'Timeout'],
  ['TRANSIENT_NETWORK', 'Network glitch'],
  ['UPSTREAM_MISSING', 'Missing input'],
  ['UNKNOWN', 'Unknown'],
]
const LABELS = Object.fromEntries(CATEGORIES)

const ML_STATUS_TEXT = {
  used: 'Model consulted',
  skipped: 'Model not needed (specific rule matched)',
  unavailable: 'Model unavailable — regex result used',
  disabled: 'Model disabled — regex only',
}

const pct = (value) => `${Math.round((value ?? 0) * 100)}%`

export function SourcePill({ diagnosis }) {
  if (diagnosis.source === 'operator') {
    return <Badge tone="info" title={diagnosis.corrected_by ? `Corrected by ${diagnosis.corrected_by}` : undefined}>Operator</Badge>
  }
  if (diagnosis.source === 'model') {
    return <Badge tone="info">Model{diagnosis.model_version ? ` ${diagnosis.model_version}` : ''}</Badge>
  }
  return <Badge tone="neutral">Regex</Badge>
}

/** Top-k model scores as plain CSS bars (no chart dependency). */
export function ScoreBars({ predictions, highlight }) {
  if (!predictions?.length) return null
  return (
    <ul className="score-bars" aria-label="Model scores">
      {predictions.map((p) => (
        <li key={p.category} className={p.category === highlight ? 'score-bar-top' : ''}>
          <span className="score-label small">{LABELS[p.category] ?? p.category}</span>
          <span className="score-track" aria-hidden="true">
            <span className="score-fill" style={{ width: pct(p.score) }} />
          </span>
          <span className="score-value small mono">{pct(p.score)}</span>
        </li>
      ))}
    </ul>
  )
}

export function DiagnosisSummary({ diagnosis }) {
  return (
    <>
      <div className="cell-title">
        <strong>{diagnosis.label}</strong>
        <SourcePill diagnosis={diagnosis} />
        <Badge tone={diagnosis.retryable ? 'success' : 'warning'}>
          {diagnosis.retryable ? 'a retry may help' : 'a retry will not help'}
        </Badge>
        {diagnosis.confidence > 0 && diagnosis.source !== 'operator' && (
          <span className="muted small">{pct(diagnosis.confidence)} confidence</span>
        )}
      </div>
      {diagnosis.suggestion && (
        <p className="small diagnosis-suggestion">
          Suggested by the model: <strong>{diagnosis.suggestion.label ?? LABELS[diagnosis.suggestion.category]}</strong>{' '}
          ({pct(diagnosis.suggestion.confidence)}) — below the confidence needed to act on it.
        </p>
      )}
      {diagnosis.matched_line ? (
        <pre className="log-viewer log-inline">{diagnosis.matched_line}</pre>
      ) : (
        <p className="muted small">
          {diagnosis.rule === 'no_log' ? 'No failure log was available.' : 'No known error pattern matched the log.'}
        </p>
      )}
    </>
  )
}

function CorrectModal({ diagnosis, onClose, onSave }) {
  const [category, setCategory] = useState(diagnosis.category)
  const [note, setNote] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  async function submit(event) {
    event.preventDefault()
    setSaving(true)
    try {
      await onSave(category, note.trim() || null)
    } catch (err) {
      setError(err.message)
      setSaving(false)
    }
  }

  return (
    <Modal
      title="Correct diagnosis"
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="correct-diagnosis-form" loading={saving}>
            Save correction
          </Button>
        </>
      }
    >
      <form id="correct-diagnosis-form" className="form-grid" onSubmit={submit}>
        {error && <Alert tone="danger">{error}</Alert>}
        <Field label="Category" hint="Workflows and retry decisions use this from now on. New evidence will not overwrite it.">
          {(id) => (
            <select id={id} value={category} onChange={(e) => setCategory(e.target.value)} autoFocus>
              {CATEGORIES.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          )}
        </Field>
        <Field label="Note (optional)" hint="Why — helps when this is used as training data.">
          {(id) => <textarea id={id} rows={3} value={note} onChange={(e) => setNote(e.target.value)} maxLength={2000} />}
        </Field>
      </form>
    </Modal>
  )
}

export default function DiagnosisCard({ diagnosis, canCorrect, onCorrect }) {
  const [correcting, setCorrecting] = useState(false)
  const showScores = diagnosis.top_predictions?.length > 0
  return (
    <Card
      title="Diagnosis"
      actions={
        canCorrect && (
          <Button size="sm" onClick={() => setCorrecting(true)}>
            Correct category
          </Button>
        )
      }
    >
      <DiagnosisSummary diagnosis={diagnosis} />
      {diagnosis.source === 'operator' && (
        <p className="small muted">
          Corrected{diagnosis.corrected_by ? ` by ${diagnosis.corrected_by}` : ''}
          {diagnosis.previous?.category && ` (was ${LABELS[diagnosis.previous.category] ?? diagnosis.previous.category}, ${diagnosis.previous.source})`}
          {diagnosis.note && <> — “{diagnosis.note}”</>}
        </p>
      )}
      {showScores && (
        <details className="diagnosis-details">
          <summary className="small">Model scores{diagnosis.model_version ? ` · ${diagnosis.model_version}` : ''}</summary>
          <ScoreBars predictions={diagnosis.top_predictions} highlight={diagnosis.category} />
        </details>
      )}
      {diagnosis.ml_status && diagnosis.source !== 'operator' && (
        <p className="muted small">{ML_STATUS_TEXT[diagnosis.ml_status] ?? diagnosis.ml_status}</p>
      )}
      {correcting && (
        <CorrectModal
          diagnosis={diagnosis}
          onClose={() => setCorrecting(false)}
          onSave={async (category, note) => {
            await onCorrect(category, note)
            setCorrecting(false)
          }}
        />
      )}
    </Card>
  )
}
