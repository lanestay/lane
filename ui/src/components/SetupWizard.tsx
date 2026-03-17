import { useState } from "react";
import { performSetup } from "../lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";

type Step = "welcome" | "form" | "success";

export default function SetupWizard({ onComplete }: { onComplete: () => void }) {
  const [step, setStep] = useState<Step>("welcome");
  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [phone, setPhone] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [apiKey, setApiKey] = useState("");

  const submit = async () => {
    setError("");

    if (!email.trim()) { setError("Email is required"); return; }
    if (password.length < 8) { setError("Password must be at least 8 characters"); return; }
    if (password !== confirmPassword) { setError("Passwords do not match"); return; }

    setLoading(true);
    try {
      const result = await performSetup({
        email: email.trim(),
        display_name: displayName.trim() || undefined,
        password,
        phone: phone.trim() || undefined,
      });
      setApiKey(result.api_key);
      setStep("success");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  if (step === "welcome") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Card className="max-w-md w-full mx-4">
          <CardHeader>
            <CardTitle className="text-2xl">Welcome to lane</CardTitle>
            <CardDescription>Create your admin account to get started.</CardDescription>
          </CardHeader>
          <CardContent>
            <Button className="w-full" onClick={() => setStep("form")}>
              Get Started
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (step === "success") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Card className="max-w-md w-full mx-4">
          <CardHeader>
            <CardTitle className="text-2xl">Setup Complete</CardTitle>
            <CardDescription>
              Your admin account has been created. Save the API key below for programmatic/MCP access.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>System API Key</Label>
              <div className="flex gap-2">
                <code className="flex-1 bg-muted p-2 rounded-md text-xs break-all select-all block">
                  {apiKey}
                </code>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => navigator.clipboard.writeText(apiKey)}
                >
                  Copy
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Use this key in x-lane-key headers or MCP configuration. You can find it later in the data directory.
              </p>
            </div>
            <Button className="w-full" onClick={onComplete}>
              Continue to Login
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Form step
  return (
    <div className="min-h-screen flex items-center justify-center">
      <Card className="max-w-md w-full mx-4">
        <CardHeader>
          <CardTitle className="text-2xl">Create Admin Account</CardTitle>
          <CardDescription>This will be the first admin user for your instance.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {error && (
            <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
              {error}
            </div>
          )}
          <div className="space-y-2">
            <Label htmlFor="setup-email">Email</Label>
            <Input
              id="setup-email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="admin@example.com"
              autoFocus
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="setup-name">Display Name (optional)</Label>
            <Input
              id="setup-name"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="Admin"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="setup-phone">Phone (optional, for future 2FA)</Label>
            <Input
              id="setup-phone"
              type="tel"
              value={phone}
              onChange={(e) => setPhone(e.target.value)}
              placeholder="+1 555-0100"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="setup-password">Password</Label>
            <Input
              id="setup-password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Min. 8 characters"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="setup-confirm">Confirm Password</Label>
            <Input
              id="setup-confirm"
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              placeholder="Confirm password"
              onKeyDown={(e) => e.key === "Enter" && submit()}
            />
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={() => setStep("welcome")}>Back</Button>
            <Button className="flex-1" onClick={submit} disabled={loading}>
              {loading ? "Creating..." : "Create Admin Account"}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
