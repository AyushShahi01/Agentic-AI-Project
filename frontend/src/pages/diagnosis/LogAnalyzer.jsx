import { useState } from 'react'
import { DiagnosisSummary, ScoreBars } from '../../components/diagnosis/DiagnosisCard'
import { Alert, Badge, Button, Card, Field } from '../../components/ui'
import { diagnosisApi } from '../../services/endpoints'

const MAX_BYTES = 256 * 1024

const STATUS = {
  used: ['info', 'Model consulted'],
  skipped: ['neutral', 'Model not needed: a specific rule matched'],
  unavailable: ['warning', 'Model unavailable: regex result used'],
  disabled: ['neutral', 'Model disabled: regex only'],
}

function decisionText(result) {
  const { final, regex, model, threshold } = result
  const t = `${Math.round(threshold * 100)}%`
  if (result.ml_status === 'skipped') return `Regex matched a specific rule (${regex.rule}), so it is used as-is.`
  if (result.ml_status !== 'used' || !model) return 'Only the regex result is available.'
  if (final.source === 'model') return `Regex was weak (${regex.label}) and the model was confident (≥ ${t}), so the model's category is used.`
  return `The model's confidence was below ${t}, so the regex category is kept${final.suggestion ? ' and the model answer is shown as a suggestion' : ''}.`
}

export default function LogAnalyzer() {
  const [log, setLog] = useState('')
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const tooBig = new Blob([log]).size > MAX_BYTES

  async function analyze(event) {
    event.preventDefault()
    if (!log.trim() || tooBig) return
    setLoading(true)
    setError(null)
    try {
      setResult(await diagnosisApi.classifyLog(log))
    } catch (err) {
      setError(err.message)
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  const [tone, statusText] = result ? (STATUS[result.ml_status] ?? ['neutral', result.ml_status]) : []

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Log analyzer</h1>
          <p className="muted">
            Paste a task log to see how it would be diagnosed: the regex rules, the model, and the hybrid decision.
            Nothing is saved.
          </p>
        </div>
      </div>

      <Card>
        <form className="form-grid" onSubmit={analyze}>
          <Field label="Task log" error={tooBig ? 'The log is larger than 256 KB; paste the end of it.' : undefined}>
            {(id) => (
              <textarea
                id={id}
                className="mono analyzer-input"
                rows={12}
                value={log}
                onChange={(e) => setLog(e.target.value)}
                placeholder="[2026-10-01, 12:00:00 UTC] {taskinstance.py:2905} ERROR - Task failed with exception…"
                spellCheck={false}
              />
            )}
          </Field>
          <div className="card-actions">
            <Button variant="primary" type="submit" loading={loading} disabled={!log.trim() || tooBig}>
              Analyze
            </Button>
            {log && (
              <Button variant="ghost" onClick={() => { setLog(''); setResult(null) }}>
                Clear
              </Button>
            )}
          </div>
        </form>
      </Card>

      {error && <Alert tone="danger">{error}</Alert>}

      {result && (
        <>
          <Card title="Decision" actions={<Badge tone={tone}>{statusText}</Badge>}>
            <DiagnosisSummary diagnosis={result.final} />
            <p className="small muted">{decisionText(result)}</p>
          </Card>

          <div className="detail-grid">
            <Card title="Regex rules">
              <DiagnosisSummary diagnosis={result.regex} />
              {result.regex.rule && <p className="muted small mono">rule: {result.regex.rule}</p>}
            </Card>
            <Card title="Model">
              {result.model ? (
                <>
                  <div className="cell-title">
                    <strong>{result.model.category}</strong>
                    <span className="muted small">{Math.round(result.model.confidence * 100)}% confidence</span>
                  </div>
                  <ScoreBars predictions={result.model.top_predictions} highlight={result.model.category} />
                  <p className="muted small">
                    {[result.model.model_version, result.model.device,
                      result.model.inference_ms != null && `${Math.round(result.model.inference_ms)} ms`]
                      .filter(Boolean)
                      .join(' · ')}
                  </p>
                </>
              ) : (
                <p className="muted small">No model answer ({statusText.toLowerCase()}).</p>
              )}
            </Card>
          </div>

          {result.window && (
            <Card title="What the model saw">
              <p className="muted small">
                The log after cleanup (timestamps stripped, ids and paths masked) and windowed to the model's
                512-token limit.
              </p>
              <pre className="log-viewer">{result.window}</pre>
            </Card>
          )}
        </>
      )}
    </div>
  )
}
