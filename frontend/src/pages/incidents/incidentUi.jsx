import { Badge } from '../../components/ui'

const SEVERITY_TONES = { LOW: 'neutral', MEDIUM: 'warning', HIGH: 'danger', CRITICAL: 'danger' }
const STATUS_TONES = { OPEN: 'danger', ACKNOWLEDGED: 'warning', RESOLVED: 'success' }
const RESOLUTION_LABELS = { AUTO_RECOVERED: 'auto-recovered', AUTO_REMEDIATED: 'fixed by automation' }

export function SeverityBadge({ severity }) {
  return (
    <span className={`severity severity-${severity.toLowerCase()}`}>
      <Badge tone={SEVERITY_TONES[severity]}>{severity.toLowerCase()}</Badge>
    </span>
  )
}

export function IncidentStatusBadge({ status, resolution }) {
  const label = (status === 'RESOLVED' && RESOLUTION_LABELS[resolution]) || status.toLowerCase()
  return <Badge tone={STATUS_TONES[status]}>{label}</Badge>
}

