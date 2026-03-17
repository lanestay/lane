# PII Detection & Redaction

Lane can automatically detect and redact Personally Identifiable Information in query results.

## PII Modes

| Mode | Behavior | Example (SSN) |
|------|----------|---------------|
| **None** | No redaction | `123-45-6789` |
| **Scrub** | Replace with entity type | `<ssn>` |

## Built-in Entity Detection

| Entity | Pattern | Validation |
|--------|---------|------------|
| **SSN** | `\d{3}-\d{2}-\d{4}` or 9 digits | Not 000/666/9xx prefix, valid group/serial |
| **Credit Card** | 13-19 digit sequences | Luhn algorithm + plausible IIN (Visa, Amex, etc.) |
| **Email** | RFC-style `user@domain.tld` | Standard pattern matching |
| **Phone** | US format with optional +1 | `(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}` |

## Column Tagging

Admins can explicitly tag columns as containing PII, forcing redaction regardless of auto-detection:

```
POST /api/lane/admin/pii-columns
Body: {
  "connection_name": "prod",
  "database_name": "customers",
  "schema_name": "dbo",
  "table_name": "users",
  "column_name": "ssn",
  "pii_type": "ssn"
}
```

Tagged columns are always redacted when PII mode is active, even if the regex doesn't match (e.g., non-standard formats).

## Custom Rules

Create regex-based detection rules beyond the built-ins:

```
POST /api/lane/admin/pii-rules
Body: {
  "name": "employee_id",
  "description": "Internal employee ID format",
  "regex_pattern": "EMP-\\d{6}",
  "replacement_text": "<employee_id>",
  "entity_kind": "custom"
}
```

Admin endpoints:
- `GET /api/lane/admin/pii/rules` — list all rules
- `POST /api/lane/admin/pii/rules` — create rule
- `PUT /api/lane/admin/pii/rules/{id}` — update
- `DELETE /api/lane/admin/pii/rules/{id}` — delete
- `POST /api/lane/admin/pii/rules/test` — test rule against sample text

## Per-User/Token PII Mode

- Each user can have a `pii_mode` set in their profile
- Each [[auth#User Token|token]] can override with its own `pii_mode`
- Token PII mode takes precedence over user PII mode

## Related

- [[auth]] — Per-token PII mode overrides
- [[permissions]] — User profile PII mode setting
- [[api]] — PII admin endpoints
