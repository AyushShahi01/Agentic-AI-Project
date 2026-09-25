/**
 * Shared JSDoc typedefs mirroring the backend schemas (backend/app/schemas).
 *
 * @typedef {'ADMIN' | 'OPERATOR' | 'VIEWER'} Role
 *
 * @typedef {Object} User
 * @property {string} id
 * @property {string} email
 * @property {string} full_name
 * @property {Role} role
 * @property {boolean} is_active
 * @property {string | null} last_login_at
 *
 * @typedef {'UNKNOWN' | 'HEALTHY' | 'UNAUTHORIZED' | 'FORBIDDEN' | 'UNREACHABLE' |
 *   'TLS_ERROR' | 'INVALID_ENDPOINT' | 'AIRFLOW_ERROR'} ConnectionStatus
 *
 * @typedef {Object} AirflowConnection
 * @property {string} id
 * @property {string} name
 * @property {'DEV' | 'STAGING' | 'PROD'} environment
 * @property {'LIVE' | 'MOCK'} kind
 * @property {string} base_url
 * @property {'BASIC' | 'TOKEN' | 'NONE'} auth_type
 * @property {string | null} username
 * @property {boolean} has_secret
 * @property {'v1' | 'v2' | null} api_version
 * @property {string | null} airflow_version
 * @property {boolean} is_active
 * @property {boolean} is_default
 * @property {ConnectionStatus} last_health_status
 * @property {string | null} last_health_message
 * @property {number | null} last_latency_ms
 * @property {string | null} last_checked_at
 *
 * @typedef {Object} ConnectionTestResult
 * @property {ConnectionStatus} status
 * @property {boolean} ok
 * @property {string} message
 * @property {number | null} latency_ms
 * @property {'v1' | 'v2' | null} api_version
 * @property {string | null} airflow_version
 * @property {string} checked_at
 *
 * @typedef {Object} MonitoredDag
 * @property {string} id
 * @property {string} connection_id
 * @property {string} dag_id
 * @property {string | null} description
 * @property {string | null} schedule_summary
 * @property {boolean | null} is_paused
 * @property {boolean} is_monitored
 * @property {boolean} is_present
 * @property {string[]} tags
 * @property {string | null} last_synced_at
 *
 * @typedef {Object} Page
 * @property {Array<any>} items
 * @property {number} total
 * @property {number} limit
 * @property {number} offset
 */

export const ROLES = /** @type {const} */ (['ADMIN', 'OPERATOR', 'VIEWER'])
