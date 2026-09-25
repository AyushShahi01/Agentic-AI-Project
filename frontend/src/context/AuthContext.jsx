import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { refreshSession, setAccessToken, setAuthLostHandler } from '../services/apiClient'
import { authApi } from '../services/endpoints'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [status, setStatus] = useState('loading') // loading | authenticated | anonymous

  useEffect(() => {
    setAuthLostHandler(() => {
      setUser(null)
      setStatus('anonymous')
    })
    // Restore the session from the refresh cookie (the access token is memory-only).
    refreshSession()
      .then((body) => {
        setUser(body.user)
        setStatus('authenticated')
      })
      .catch(() => setStatus('anonymous'))
  }, [])

  const login = useCallback(async (email, password) => {
    const body = await authApi.login(email, password)
    setUser(body.user)
    setStatus('authenticated')
    return body.user
  }, [])

  const logout = useCallback(async () => {
    try {
      await authApi.logout()
    } finally {
      setAccessToken(null)
      setUser(null)
      setStatus('anonymous')
    }
  }, [])

  const hasRole = useCallback((...roles) => Boolean(user && roles.includes(user.role)), [user])

  const value = useMemo(
    () => ({ user, status, login, logout, hasRole }),
    [user, status, login, logout, hasRole],
  )
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

// eslint-disable-next-line react/only-export-components
export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used inside <AuthProvider>')
  return ctx
}
