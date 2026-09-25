import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router'
import { RedirectIfAuthenticated, RequireAuth, RequireRole } from './components/RouteGuards'
import AppLayout from './layouts/AppLayout'
import AuthLayout from './layouts/AuthLayout'
import Login from './pages/auth/Login'
import Approvals from './pages/automation/Approvals'
import Notifications from './pages/automation/Notifications'
import RunDetail from './pages/automation/RunDetail'
import RunList from './pages/automation/RunList'
import Workflows from './pages/automation/Workflows'
import Dashboard from './pages/dashboard/Dashboard'
import IncidentDetail from './pages/incidents/IncidentDetail'
import IncidentList from './pages/incidents/IncidentList'
import Connections from './pages/settings/Connections'
import MonitoredDags from './pages/settings/MonitoredDags'
import Users from './pages/settings/Users'

// Canvas pages pull in React Flow; load them only when opened.
const WorkflowEditor = lazy(() => import('./pages/automation/WorkflowEditor'))
const PipelineCanvas = lazy(() => import('./pages/pipelines/PipelineCanvas'))

function Lazy({ children }) {
  return <Suspense fallback={<p className="muted">Loading…</p>}>{children}</Suspense>
}

export default function App() {
  return (
    <Routes>
      <Route element={<RedirectIfAuthenticated />}>
        <Route element={<AuthLayout />}>
          <Route path="/login" element={<Login />} />
        </Route>
      </Route>

      <Route element={<RequireAuth />}>
        <Route element={<AppLayout />}>
          <Route index element={<Dashboard />} />
          <Route path="/incidents" element={<IncidentList />} />
          <Route path="/incidents/:id" element={<IncidentDetail />} />
          <Route path="/automation/approvals" element={<Approvals />} />
          <Route path="/automation/runs" element={<RunList />} />
          <Route path="/automation/runs/:id" element={<RunDetail />} />
          <Route path="/automation/workflows" element={<Workflows />} />
          <Route path="/automation/workflows/:id" element={<Lazy><WorkflowEditor /></Lazy>} />
          <Route path="/pipelines" element={<Lazy><PipelineCanvas /></Lazy>} />
          <Route path="/automation/notifications" element={<Notifications />} />
          <Route path="/settings/connections" element={<Connections />} />
          <Route path="/settings/dags" element={<MonitoredDags />} />
          <Route element={<RequireRole roles={['ADMIN']} />}>
            <Route path="/settings/users" element={<Users />} />
          </Route>
        </Route>
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
