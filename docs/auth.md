# Authentication

Lane supports multiple authentication methods, checked in order on each request.

## Auth Providers

Authentication providers are configured via the `LANE_AUTH` env var (comma-separated). Default is `email`.

```bash
LANE_AUTH=email                     # password login (+ code login if SMTP configured)
LANE_AUTH=tailscale                 # tailnet identity headers
LANE_AUTH=email,google              # password/code + Google SSO
LANE_AUTH=email,google,github       # password/code + Google + GitHub
```

All users must be pre-created by an admin before they can sign in via any provider. See the [AUTH.md](../AUTH.md) guide for provider setup instructions.

OIDC providers (Google, Microsoft, GitHub) require additional env vars:

```bash
LANE_BASE_URL=https://your-app.example.com
LANE_GOOGLE_CLIENT_ID=...
LANE_GOOGLE_CLIENT_SECRET=...
LANE_MICROSOFT_CLIENT_ID=...
LANE_MICROSOFT_CLIENT_SECRET=...
LANE_GITHUB_CLIENT_ID=...
LANE_GITHUB_CLIENT_SECRET=...
```

## Auth Methods (Request Evaluation Order)

### 1. System API Key (Bootstrap / Break-Glass Only)

- Header: `x-api-key` or `x-lane-key`
- Grants: `FullAccess` — unrestricted access to everything, bypasses all permission checks
- Auto-generated on first startup, stored in the encrypted auth database
- **Purpose**: initial setup (creating the first admin user) and emergency access if auth is misconfigured
- **Do not** use for day-to-day operations — use session auth, user tokens, or service accounts instead
- **Keep it secret** — anyone with this key has full control over the system. Store it securely and never share it. Rotate immediately if compromised.

### 2. Service Account Key

- Header: `x-api-key` or `x-lane-key`
- Grants: `ServiceAccountAccess { account_name }`
- Subject to [[permissions]] (sql_mode, database/table, connection, storage)
- Can be rotated via admin API
- See [[permissions#Service Account Permissions]]

### 3. User Token (Personal Access Token)

- Header: `x-api-key` or `x-lane-key`
- Grants: `TokenAccess { email, pii_mode }`
- 64-character random hex string
- Optional expiry and per-token [[pii]] mode override
- Revocable by prefix (first 16 chars) or full token

### 4. Tailscale Identity

- Headers: `Tailscale-User-Login` / `Tailscale-User-Name` (injected by `tailscale serve`)
- Grants: `SessionAccess { email, is_admin }`
- Only checked when `tailscale` is in `LANE_AUTH`
- User must already exist in the auth DB with a matching email

### 5. Session Token (Web UI)

- Header: `Authorization: Bearer <token>` or cookie `session=<token>`
- Grants: `SessionAccess { email, is_admin }`
- Created via email+password login, email code login, or OIDC callback
- 24-hour expiry, tracks client IP and User-Agent
- OIDC sessions use `SameSite=Lax` cookies (cross-origin redirect requirement)

## Auth Flow

```
Request → authenticate(headers, state)
  1. Check x-api-key → system API key? → FullAccess
  2. Check x-api-key → service account key? → ServiceAccountAccess
  3. Check x-api-key → user token? → TokenAccess
  4. Tailscale enabled? Check identity headers → user exists? → SessionAccess
  5. Check session (Bearer header or cookie) → SessionAccess
  6. None matched → Denied
```

## Session Management

### Email+Password Login

```
POST /api/auth/login
Body: { "email": "...", "password": "..." }
Response: { "session_token": "...", "email": "...", "is_admin": true }
```

Sets `HttpOnly`, `SameSite=Strict` cookie with 24h expiry.

### Email Code Login (Passwordless)

When SMTP is configured, email users can sign in with a 6-digit code instead of a password. This is useful for users created via OIDC who don't have a password set.

```bash
# Required to enable code login
LANE_SMTP_HOST=smtp.gmail.com       # any SMTP server
LANE_SMTP_PORT=587                  # default: 587
LANE_SMTP_TLS=starttls              # none | starttls (default) | tls
LANE_SMTP_FROM=noreply@example.com  # default: noreply@lane.local
LANE_SMTP_USERNAME=                 # optional
LANE_SMTP_PASSWORD=                 # optional
```

```
POST /api/auth/email-code/send
Body: { "email": "..." }
Response: { "success": true }   (always, to prevent email enumeration)

POST /api/auth/email-code/verify
Body: { "email": "...", "code": "123456" }
Response: { "session_token": "...", "email": "...", "is_admin": true }
```

Security:
- 6-digit codes are SHA-256 hashed before storage
- Codes expire after 10 minutes
- Max 3 verification attempts per code
- Max 5 codes per email per hour
- Send endpoint always returns success (anti-enumeration)
- `GET /api/auth/status` includes `smtp_configured: true` when enabled

For local testing, uncomment the Mailpit service in `docker-compose.full.yml` and set `SMTP_HOST=mailpit`, `PORT=1025`, `TLS=none`. Mailpit catches all outgoing mail at `http://localhost:8025`.

### OIDC Login (Google / Microsoft / GitHub)

```
GET /api/auth/oidc/{provider}/authorize   → redirect to provider
GET /api/auth/oidc/{provider}/callback    → exchange code, create session, redirect to /
```

Uses PKCE (S256) and single-use state parameter (10-minute expiry). Sets `HttpOnly`, `SameSite=Lax` cookie with 24h expiry.

### Tailscale Login

```
POST /api/auth/tailscale-login
```

Extracts identity from Tailscale headers and creates a session.

### Logout

```
POST /api/auth/logout
```

Destroys the session server-side and clears the cookie.

### Password Change

```
POST /api/auth/password
Body: { "current_password": "...", "new_password": "..." }
```

Passwords are hashed with Argon2 + random salt. Minimum 8 characters.

### Purge User Sessions (Admin)

```
DELETE /api/lane/admin/users/{email}/sessions
```

Destroys all active sessions for a user. Requires admin access.

## Token Management

### Self-Service

Users can manage their own tokens:

- `POST /api/lane/self/tokens` — generate (returns full token once)
- `GET /api/lane/self/tokens` — list own tokens
- `POST /api/lane/self/tokens/{prefix}/revoke` — revoke by prefix

### Admin

Admins can manage tokens for any user:

- `POST /api/lane/admin/tokens/generate` — generate for user
- `GET /api/lane/admin/tokens?email={email}` — list user's tokens
- `POST /api/lane/admin/tokens/revoke` — revoke by prefix
- `GET/POST /api/lane/admin/token-policy` — system token expiry settings

## Auth Database

The auth database is encrypted with SQLCipher. It stores:

- Users, passwords, permissions
- Tokens and sessions
- Service accounts
- OAuth states (PKCE verifiers, CSRF tokens)
- Email login codes (hashed, with attempt tracking)
- [[approvals]] state
- [[pii]] rules and column tags
- [[teams]] and project membership
- Access logs and query history

Encryption key is auto-generated on first startup (32 random bytes) and stored at `{LANE_DATA_DIR}/.cipher_key` with `0600` permissions.

## Related

- [[permissions]] — What authenticated users can do
- [[approvals]] — Write approval workflows
- [[pii]] — Per-token PII mode overrides
