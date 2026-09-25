import { useState } from 'react'
import { useLocation, useNavigate } from 'react-router'
import { Alert, Button, Field } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'

export default function Login() {
  const { login } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState(null)
  const [submitting, setSubmitting] = useState(false)

  async function onSubmit(event) {
    event.preventDefault()
    setError(null)
    if (!email.includes('@')) return setError('Enter a valid email address.')
    if (!password) return setError('Enter your password.')
    setSubmitting(true)
    try {
      await login(email.trim(), password)
      navigate(location.state?.from?.pathname ?? '/', { replace: true })
    } catch (err) {
      if (err.code === 'rate_limited') {
        const minutes = Math.ceil((err.details?.retry_after_seconds ?? 60) / 60)
        setError(`Too many failed attempts. Try again in about ${minutes} minute(s).`)
      } else {
        setError(err.message)
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form className="auth-form" onSubmit={onSubmit} noValidate>
      <h1>Sign in</h1>
      <p className="muted">Accounts are created by an administrator.</p>
      {error && <Alert tone="danger">{error}</Alert>}
      <Field label="Email">
        {(id) => (
          <input
            id={id}
            type="email"
            autoComplete="username"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoFocus
          />
        )}
      </Field>
      <Field label="Password">
        {(id) => (
          <input
            id={id}
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        )}
      </Field>
      <Button type="submit" variant="primary" loading={submitting} className="btn-block">
        Sign in
      </Button>
    </form>
  )
}
