# Approvals

Users in **Supervised** or **Confirmed** [[permissions#SqlMode Tiers|SqlMode]] must have write operations approved before execution.

## How It Works

1. User submits a write query (via [[api]] or [[mcp]])
2. System detects DML and checks SqlMode
3. If approval needed → creates a `PendingApproval` in the in-memory registry
4. User (or approver) is notified via SSE or webhook
5. Approver reviews the SQL and approves or rejects
6. Original request completes or fails based on decision
7. Approval times out after **5 minutes** if no action taken

## Who Can Approve

| Approver | Supervised User | Confirmed User |
|----------|----------------|----------------|
| The user themselves | No (self-block) | Yes |
| Org admin (`is_admin`) | Yes | Yes |
| Team lead (same [[teams|team]]) | Yes | Yes |
| Project lead (same [[teams|project]]) | Yes | Yes |

## Per-User Limits

Each user has a `max_pending_approvals` limit (default: 6). Submitting beyond this limit returns an error.

## Approval Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/approvals` | GET | List pending approvals |
| `/api/lane/approvals/{id}` | GET | Get details (includes full SQL) |
| `/api/lane/approvals/{id}/approve` | POST | Approve |
| `/api/lane/approvals/{id}/reject` | POST | Reject (with reason) |
| `/api/lane/approvals/events` | GET (SSE) | Real-time approval notifications |

## SSE Events

The `/approvals/events` endpoint streams Server-Sent Events:

- `new_approval` — a new approval is pending
- `resolved` — an approval was approved or rejected

## Webhooks

When an approval is created, webhooks fire to all [[teams]] the user belongs to (if the team has a `webhook_url` configured). Payload is Slack-compatible JSON.

## MCP Approval Flow

[[mcp]] tools that modify data (`bulk_update`, `bulk_insert`, `run_migration`, `storage_upload`) use the same approval system. The MCP tool call blocks (with 5-minute timeout) until the approval is resolved.

## Storage Approvals

Storage uploads via MCP (`storage_upload`) also go through the approval flow for Supervised/Confirmed users. The approval shows `PUT bucket/key` as the action.

## UI

The [[ui#Approvals Page]] shows pending approvals with approve/reject actions and real-time SSE updates.

## Related

- [[permissions#SqlMode Tiers]] — Which modes require approval
- [[teams]] — Approval delegation via roles
- [[mcp]] — MCP tool approval integration
