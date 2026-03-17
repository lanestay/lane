import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from "react";
import { checkAuthStatus, loginWithPassword, loginWithTailscale, logoutSession } from "./api";

interface TeamMembership {
  id: string;
  name: string;
  role: string;
}

interface AuthUser {
  email: string;
  is_admin: boolean;
  teams?: TeamMembership[];
}

interface AuthContextType {
  loading: boolean;
  needsSetup: boolean;
  authenticated: boolean;
  user: AuthUser | null;
  tailscaleAuth: boolean;
  authProviders: string[];
  smtpConfigured: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshStatus: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [loading, setLoading] = useState(true);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [authProviders, setAuthProviders] = useState<string[]>([]);
  const [smtpConfigured, setSmtpConfigured] = useState(false);

  const tailscaleAuth = authProviders.includes("tailscale");

  const refreshStatus = useCallback(async () => {
    try {
      const status = await checkAuthStatus();
      setNeedsSetup(status.needs_setup);
      setAuthenticated(status.authenticated);
      setUser(status.user);
      setSmtpConfigured(!!status.smtp_configured);
      // Derive authProviders from new field, fall back to tailscale_auth for compat
      if (status.auth_providers) {
        setAuthProviders(status.auth_providers);
      } else if (status.tailscale_auth) {
        setAuthProviders(["tailscale"]);
      } else {
        setAuthProviders(["email"]);
      }

      // Auto-login via Tailscale if enabled and not yet authenticated
      const isTailscale = status.auth_providers
        ? status.auth_providers.includes("tailscale")
        : status.tailscale_auth;
      if (isTailscale && !status.authenticated && !status.needs_setup) {
        try {
          const result = await loginWithTailscale();
          if (result.success) {
            setAuthenticated(true);
            setUser({ email: result.email, is_admin: result.is_admin });
          }
        } catch {
          // Tailscale headers not present (e.g. direct access), fall through to login form
        }
      }
    } catch {
      setAuthenticated(false);
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refreshStatus();
  }, [refreshStatus]);

  const login = async (email: string, password: string) => {
    const result = await loginWithPassword(email, password);
    // Store session token as localStorage fallback (cookie is primary)
    if (result.session_token) {
      localStorage.setItem("session_token", result.session_token);
    }
    setAuthenticated(true);
    setUser({ email: result.email, is_admin: result.is_admin });
  };

  const logout = async () => {
    try {
      await logoutSession();
    } catch {
      // proceed even if logout call fails
    }
    localStorage.removeItem("session_token");
    setAuthenticated(false);
    setUser(null);
    // In Tailscale mode, re-check status to auto-login again
    if (authProviders.includes("tailscale")) {
      await refreshStatus();
    }
  };

  return (
    <AuthContext.Provider value={{ loading, needsSetup, authenticated, user, tailscaleAuth, authProviders, smtpConfigured, login, logout, refreshStatus }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextType {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
