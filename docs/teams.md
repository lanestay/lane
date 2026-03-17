# Teams & Projects

Lane supports a two-level org hierarchy for managing users and [[approvals]] delegation.

## Structure

```
Team (e.g., "Data Engineering")
├── Members (email + role)
├── Projects (e.g., "ETL Pipeline")
│   └── Members (email + role)
└── Webhook URL (optional, for Slack notifications)
```

## Roles

| Role | Can Approve Team Members | Can Approve Project Members |
|------|-------------------------|---------------------------|
| `member` | No | No |
| `team_lead` | Yes | Yes (for their team's projects) |
| `project_lead` | No | Yes (for their project only) |

Org admins (`is_admin`) can approve anyone regardless of team membership.

## Approval Delegation

When a Supervised user submits a write query:

1. They cannot self-approve
2. Any org admin can approve
3. A `team_lead` in their team can approve
4. A `project_lead` in their project can approve

See [[approvals]] for the full approval flow.

## Webhooks

Each team can have a `webhook_url`. When a team member creates an approval request, a Slack-compatible JSON payload is sent to the webhook (fire-and-forget, 10s timeout).

## Admin Endpoints

### Teams

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/teams` | GET | List all teams |
| `/api/lane/admin/teams` | POST | Create team |
| `/api/lane/admin/teams/{id}` | PUT | Update team |
| `/api/lane/admin/teams/{id}` | DELETE | Delete team (cascades) |
| `/api/lane/admin/teams/{id}/members` | GET | List members |
| `/api/lane/admin/teams/{id}/members` | POST | Add member |
| `/api/lane/admin/teams/{team_id}/members/{email}` | PUT | Update role |
| `/api/lane/admin/teams/{team_id}/members/{email}` | DELETE | Remove member |

### Projects

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/teams/{id}/projects` | GET | List projects |
| `/api/lane/admin/teams/{id}/projects` | POST | Create project |
| `/api/lane/admin/projects/{id}` | PUT | Update project |
| `/api/lane/admin/projects/{id}` | DELETE | Delete project |
| `/api/lane/admin/projects/{id}/members` | GET/POST | Project members |
| `/api/lane/admin/projects/{project_id}/members/{email}` | PUT/DELETE | Update/remove |

## UI

The [[ui#Admin Page]] includes team and project management with member role assignment.

## Related

- [[approvals]] — How team roles affect approval delegation
- [[permissions]] — User-level access control
