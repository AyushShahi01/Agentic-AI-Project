import { Navigate, Outlet, useLocation } from 'react-router'
import { useAuth } from '../context/AuthContext'

export function FullPageLoader() {
  return (
    <div className="full-page-loader">
      <span className="spinner spinner-lg" aria-hidden="true" />
      <span>Loading…</span>
    </div>
  )
}

export function RequireAuth() {
  const { status } = useAuth()
  const location = useLocation()
  if (status === 'loading') return <FullPageLoader />
  if (status !== 'authenticated') return <Navigate to="/login" replace state={{ from: location }} />
  return <Outlet />
}

export function RequireRole({ roles }) {
  const { hasRole } = useAuth()
  if (!hasRole(...roles)) return <Navigate to="/" replace />
  return <Outlet />
}

export function RedirectIfAuthenticated() {
  const { status } = useAuth()
  if (status === 'loading') return <FullPageLoader />
  if (status === 'authenticated') return <Navigate to="/" replace />
  return <Outlet />
}
