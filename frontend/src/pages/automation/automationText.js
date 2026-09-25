// Labels and formatting helpers shared by the automation pages.

export const NODE_LABELS = {
  'trigger.incident': 'When an incident opens or recurs',
  'trigger.incident_stale': 'When an incident is ignored',
  'condition.filter': 'Only if…',
  'diagnose.classify_log': 'Figure out why',
  'check.dag_state': 'Check the DAG',
  'approval.request': 'Ask a human',
  'action.clear_failed_tasks': 'Retry failed tasks',
  'action.trigger_dag_run': 'Start a new run',
  'action.set_dag_paused': 'Pause / unpause DAG',
  'verify.run_success': 'Check it worked',
  'incident.update': 'Update the incident',
  notify: 'Tell someone',
}

export const NODE_ICONS = {
  trigger: '⚡',
  condition: '⋔',
  diagnose: '🔎',
  check: '⋔',
  approval: '✋',
  action: '⚙',
  verify: '✓',
  incident: '✎',
  notify: '✉',
}

export function nodeIcon(type) {
  return NODE_ICONS[type.split('.')[0]] ?? '•'
}

export const CATEGORY_LABELS = {
  AUTH: 'Login or permission problem',
  DATA_INTEGRITY: 'Bad data',
  SCHEMA: 'Table or column changed',
  CODE_BUG: 'Code error',
  RESOURCE: 'Out of memory or disk',
  TIMEOUT: 'Timeout',
  TRANSIENT_NETWORK: 'Network glitch',
  UPSTREAM_MISSING: 'Missing input',
  UNKNOWN: 'Unknown',
}

/** Short human summary of a node's config, e.g. "categories: TIMEOUT, …; ≤ 2 occurrences". */
export function describeConfig(type, config = {}) {
  const parts = []
  const list = (values) => values.map((v) => CATEGORY_LABELS[v] ?? v).join(', ')
  switch (type) {
    case 'trigger.incident':
      parts.push(`on ${config.events?.join(' / ')}`)
      if (config.incident_types?.length) parts.push(config.incident_types.join(', '))
      break
    case 'trigger.incident_stale':
      parts.push(`open & unacknowledged for ${config.minutes} min`)
      break
    case 'condition.filter':
      if (config.diagnosis_categories?.length) parts.push(list(config.diagnosis_categories))
      if (config.environments?.length) parts.push(`env ${config.environments.join('/')}`)
      if (config.incident_types?.length) parts.push(config.incident_types.join(', '))
      if (config.min_severity) parts.push(`severity ≥ ${config.min_severity}`)
      if (config.min_occurrences) parts.push(`≥ ${config.min_occurrences} failures`)
      if (config.max_occurrences) parts.push(`≤ ${config.max_occurrences} failures`)
      if (config.dag_ids?.length) parts.push(`DAGs ${config.dag_ids.join(', ')}`)
      if (config.tags_any?.length) parts.push(`tags ${config.tags_any.join(', ')}`)
      break
    case 'approval.request':
      parts.push(
        config.required_environments?.length
          ? `required in ${config.required_environments.join('/')}`
          : 'always required',
      )
      parts.push(`expires after ${config.timeout_minutes} min`)
      break
    case 'action.clear_failed_tasks':
      parts.push(config.include_downstream ? 'with downstream tasks' : 'failed tasks only')
      break
    case 'action.set_dag_paused':
      parts.push(config.paused ? 'pause' : 'unpause')
      break
    case 'verify.run_success':
      parts.push(`wait up to ${config.timeout_minutes} min`)
      break
    case 'incident.update':
      parts.push(config.operation)
      break
    case 'notify':
      parts.push(`${config.channel === 'webhook' ? 'webhook' : 'in-app'}, ${config.level?.toLowerCase()}`)
      break
    default:
  }
  return parts.filter(Boolean).join(' · ')
}

export function duration(run) {
  if (!run.started_at) return '—'
  const end = run.finished_at ? new Date(run.finished_at) : new Date()
  const seconds = Math.max(0, Math.round((end - new Date(run.started_at)) / 1000))
  if (seconds < 60) return `${seconds} s`
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`
  return `${(seconds / 3600).toFixed(1)} h`
}
