import { useState, useEffect } from "react";
import { useAuth } from "../lib/auth";
import { sendEmailCode, verifyEmailCode } from "../lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";

const OIDC_PROVIDERS: Record<string, { label: string }> = {
  google: { label: "Google" },
  microsoft: { label: "Microsoft" },
  github: { label: "GitHub" },
};

const ERROR_MESSAGES: Record<string, string> = {
  account_not_found: "Your account does not exist. Contact an administrator to be added.",
};

type LoginMode = "password" | "code-entry" | "code-verify";

export default function Login() {
  const { login, authProviders, smtpConfigured, refreshStatus } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [mode, setMode] = useState<LoginMode>("password");

  // Check for error in URL query params (e.g. from OIDC callback rejection)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const errorCode = params.get("error");
    if (errorCode) {
      setError(ERROR_MESSAGES[errorCode] || errorCode);
      // Clean up URL
      window.history.replaceState({}, "", window.location.pathname);
    }
  }, []);

  const submitPassword = async () => {
    if (!email.trim() || !password) {
      setError("Email and password are required");
      return;
    }
    setLoading(true);
    setError("");
    try {
      await login(email.trim(), password);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Login failed");
    } finally {
      setLoading(false);
    }
  };

  const submitSendCode = async () => {
    if (!email.trim()) {
      setError("Email is required");
      return;
    }
    setLoading(true);
    setError("");
    try {
      await sendEmailCode(email.trim());
      setMode("code-verify");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to send code");
    } finally {
      setLoading(false);
    }
  };

  const submitVerifyCode = async () => {
    if (!code.trim()) {
      setError("Enter the code from your email");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const result = await verifyEmailCode(email.trim(), code.trim());
      if (result.session_token) {
        localStorage.setItem("session_token", result.session_token);
      }
      await refreshStatus();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Verification failed");
    } finally {
      setLoading(false);
    }
  };

  const hasEmail = authProviders.includes("email");
  const hasTailscale = authProviders.includes("tailscale");
  const oidcProviders = authProviders.filter((p) => p in OIDC_PROVIDERS);
  const hasOidc = oidcProviders.length > 0;

  // Tailscale-only mode
  if (hasTailscale && authProviders.length === 1) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Card className="max-w-md w-full mx-4">
          <CardHeader>
            <CardTitle className="text-2xl">lane</CardTitle>
            <CardDescription>Signing in via Tailscale...</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Waiting for Tailscale identity. Make sure you're accessing this through your Tailscale network.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex items-center justify-center">
      <Card className="max-w-md w-full mx-4">
        <CardHeader>
          <CardTitle className="text-2xl">lane</CardTitle>
          <CardDescription>Sign in to continue.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {error && (
            <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
              {error}
            </div>
          )}

          {hasOidc && mode === "password" && (
            <div className="space-y-2">
              {oidcProviders.map((provider) => (
                <Button
                  key={provider}
                  variant="outline"
                  className="w-full"
                  onClick={() => {
                    window.location.href = `/api/auth/oidc/${provider}/authorize`;
                  }}
                >
                  Sign in with {OIDC_PROVIDERS[provider].label}
                </Button>
              ))}
            </div>
          )}

          {hasOidc && hasEmail && mode === "password" && (
            <div className="relative">
              <div className="absolute inset-0 flex items-center">
                <span className="w-full border-t" />
              </div>
              <div className="relative flex justify-center text-xs uppercase">
                <span className="bg-card px-2 text-muted-foreground">or</span>
              </div>
            </div>
          )}

          {hasEmail && mode === "password" && (
            <>
              <div className="space-y-2">
                <Label htmlFor="login-email">Email</Label>
                <Input
                  id="login-email"
                  type="email"
                  placeholder="you@example.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  disabled={loading}
                  autoFocus={!hasOidc}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="login-password">Password</Label>
                <Input
                  id="login-password"
                  type="password"
                  placeholder="Password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && submitPassword()}
                  disabled={loading}
                />
              </div>
              <Button className="w-full" onClick={submitPassword} disabled={loading}>
                {loading ? "Signing in..." : "Sign In"}
              </Button>
              {smtpConfigured && (
                <button
                  type="button"
                  className="w-full text-sm text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => { setError(""); setMode("code-entry"); }}
                >
                  Sign in with a code instead
                </button>
              )}
            </>
          )}

          {hasEmail && mode === "code-entry" && (
            <>
              <div className="space-y-2">
                <Label htmlFor="code-email">Email</Label>
                <Input
                  id="code-email"
                  type="email"
                  placeholder="you@example.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && submitSendCode()}
                  disabled={loading}
                  autoFocus
                />
              </div>
              <Button className="w-full" onClick={submitSendCode} disabled={loading}>
                {loading ? "Sending..." : "Send Code"}
              </Button>
              <button
                type="button"
                className="w-full text-sm text-muted-foreground hover:text-foreground transition-colors"
                onClick={() => { setError(""); setMode("password"); }}
              >
                Back to password
              </button>
            </>
          )}

          {hasEmail && mode === "code-verify" && (
            <>
              <p className="text-sm text-muted-foreground">
                Code sent to <span className="font-medium text-foreground">{email}</span>
              </p>
              <div className="space-y-2">
                <Label htmlFor="login-code">Code</Label>
                <Input
                  id="login-code"
                  type="text"
                  inputMode="numeric"
                  placeholder="000000"
                  maxLength={6}
                  value={code}
                  onChange={(e) => setCode(e.target.value.replace(/\D/g, ""))}
                  onKeyDown={(e) => e.key === "Enter" && submitVerifyCode()}
                  disabled={loading}
                  autoFocus
                  className="text-center text-2xl tracking-[0.5em] font-mono"
                />
              </div>
              <Button className="w-full" onClick={submitVerifyCode} disabled={loading}>
                {loading ? "Verifying..." : "Verify"}
              </Button>
              <div className="flex justify-between">
                <button
                  type="button"
                  className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => { setError(""); setCode(""); submitSendCode(); }}
                >
                  Resend code
                </button>
                <button
                  type="button"
                  className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => { setError(""); setCode(""); setMode("password"); }}
                >
                  Back
                </button>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
