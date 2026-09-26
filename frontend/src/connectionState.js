// One place that turns an Airflow connection's server-reported health into what the UI shows.
// DAGs are shown only while `live` is true: the backend sets `is_live` when Airflow answered
// within the connection monitor's window, so cached DAG lists are never presented as current.

const FAILURES = {
  UNAUTHORIZED: { label: 'Authentication failed', hint: 'Check the username and password/token on the connection.' },
  FORBIDDEN: { label: 'Access denied', hint: 'The Airflow user lacks permission to read DAGs.' },
  TLS_ERROR: { label: 'TLS error', hint: 'The Airflow certificate could not be verified.' },
  INVALID_ENDPOINT: { label: 'Invalid endpoint', hint: 'The base URL does not point at an Airflow REST API.' },
  AIRFLOW_ERROR: { label: 'Airflow error', hint: 'Airflow responded with an error.' },
}

/**
 * @param {object} conn  AirflowConnRead from the API
 * @param {{ everConnected?: boolean }} [options]  true when DAGs were synced from it before,
 *   which turns "not connected" into "connection lost"
 * @returns {{ live: boolean, tone: 'success'|'neutral'|'warning'|'danger', label: string, hint: string, message: string|null }}
 */
export function connectionState(conn, { everConnected = false } = {}) {
  const message = conn.last_health_message ?? null
  if (!conn.is_active) {
    return { live: false, tone: 'neutral', label: 'Inactive', hint: 'This connection is switched off.', message: null }
  }
  if (conn.is_live) {
    return { live: true, tone: 'success', label: 'Connected', hint: '', message: null }
  }
  if (conn.status_stale) {
    return {
      live: false,
      tone: 'warning',
      label: 'Status unknown',
      hint: 'The connection has not been checked recently. Make sure the backend is running.',
      message,
    }
  }
  const status = conn.last_health_status
  if (status === 'UNKNOWN') {
    return { live: false, tone: 'neutral', label: 'Checking connection…', hint: 'Waiting for the first health check.', message: null }
  }
  if (status === 'UNREACHABLE') {
    const lost = everConnected || conn.airflow_version != null
    return {
      live: false,
      tone: 'danger',
      label: lost ? 'Connection lost' : 'Airflow not connected',
      hint: lost
        ? 'Airflow stopped responding. Pipelines reappear automatically when it is back.'
        : 'Airflow could not be reached. Check that it is running and the base URL is correct.',
      message,
    }
  }
  // HEALTHY is always live unless stale (handled above), so everything left is a failure.
  const failure = FAILURES[status] ?? { label: 'Not connected', hint: '' }
  return { live: false, tone: 'danger', ...failure, message }
}
