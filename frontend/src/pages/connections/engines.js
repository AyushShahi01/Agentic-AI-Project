// Database engines the catalog supports, with their default ports.
export const ENGINES = {
  POSTGRESQL: { label: 'PostgreSQL', port: 5432 },
  MYSQL: { label: 'MySQL', port: 3306 },
}

// Notification channel types, with where to find the link or settings.
export const CHANNEL_KINDS = {
  EMAIL: { label: 'Email', hint: 'An email account the platform sends from (SMTP).' },
  SLACK: { label: 'Slack', hint: 'Slack → Apps → Incoming Webhooks → add to a channel, then copy the link.' },
  TEAMS: {
    label: 'Microsoft Teams',
    hint: 'Teams channel → ⋯ → Workflows → “Post to a channel when a webhook request is received”, then copy the link.',
  },
  WEBHOOK: { label: 'Webhook', hint: 'Any tool that accepts a JSON POST (PagerDuty, n8n, your own service).' },
}
