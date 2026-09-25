import { Outlet } from 'react-router'

export default function AuthLayout() {
  return (
    <div className="auth-shell">
      <div className="auth-panel">
        <div className="brand brand-lg">
          <span className="brand-mark" aria-hidden="true">◆</span>
          <span>Agentic Data Automation</span>
        </div>
        <Outlet />
      </div>
    </div>
  )
}
