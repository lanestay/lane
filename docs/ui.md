# UI Pages

Lane includes a React UI served from the binary. All pages require [[auth|authentication]] unless noted.

## Global Search (Cmd+K)

Press **Cmd+K** (Mac) or **Ctrl+K** (Windows/Linux) anywhere to open the search dialog. Also accessible via the search button in the header.

- Searches across database schema, query history, and named endpoints
- Results grouped by type: **Schema**, **Query History**, **Endpoints**
- Click a schema result to navigate with connection/database pre-selected
- Click a query result to load the SQL into the editor
- Click an endpoint result to go to the admin page
- See [[search]] for details

## Query Page

**Route**: `/` (default)

The main SQL editor with:
- Connection and database selector dropdowns
- Monaco-style SQL editor with syntax highlighting
- Results table with sorting, filtering, and pagination
- **Chart view** — switch to the Chart tab to visualize results (bar, line, scatter, pie)
- Query history sidebar with favorites
- Export to CSV, JSON, or Excel
- [[pii]] redaction applied to results

### Charts

After running a query, click the **Chart** tab in the results area:
- **Chart types**: Bar, Line, Scatter, Pie
- **Axis selection**: Choose X-axis column and one or more Y-axis (numeric) columns
- Automatic detection of numeric columns for Y-axis candidates
- Theme-aware colors that match light/dark mode
- Powered by Recharts

## Tables Page

**Route**: `/tables`

Database schema browser:
- Tree view of databases → schemas → tables → columns
- Column details: name, type, nullable, primary key
- Click table name to generate `SELECT *` query
- Searchable via [[search|Cmd+K global search]]

## Objects Page

**Route**: `/objects`

Stored procedure and view browser:
- Lists procedures, views, and functions per database
- View definition/source code
- Execute procedures with parameter inputs

## Realtime Page

**Route**: `/realtime`

Live query monitoring:
- Currently active queries with duration
- SSE-powered real-time updates
- Query start/complete/error event stream
- See [[realtime]] for details

## Import Page

**Route**: `/import`

Data import wizard:
- Upload CSV, JSON, or Parquet files
- Preview schema and sample rows before import
- Import into [[workspace]] tables
- Column type detection and mapping

## Workspace Page

**Route**: `/workspace`

DuckDB workspace interface:
- List workspace tables with row counts
- Query workspace tables with SQL editor
- Export tables as CSV/JSON/Excel
- Drop tables
- See [[workspace]] for details

## Monitor Page

**Route**: `/monitor`

Query statistics dashboard:
- Query count, average duration, error rate
- Per-connection and per-database breakdowns
- Time-series charts

## Health Page

**Route**: `/health`

Connection health overview:
- Status of each database connection (pool health)
- Storage connection status
- Last successful connection time

## Storage Page

**Route**: `/storage`

MinIO/S3 file browser:
- Connection and bucket selector
- Folder-style navigation with breadcrumbs
- File upload (drag-and-drop or file picker)
- Inline preview for CSV, JSON, Parquet, text, images
- **Import to Workspace** — load a file directly into a DuckDB [[workspace]] table (CSV, Parquet, JSON, XLSX)
- Object metadata display (size, type, modified, ETag)
- Bucket creation and deletion
- See [[storage]] for details

### Save to Storage Dialog

Available from the results toolbar (alongside Export), the **Save to Storage** dialog lets you export query results or workspace data directly to a storage bucket:
- Storage connection and bucket picker
- Object key (path) input
- Format selector (CSV, JSON, XLSX for queries; CSV, JSON, Parquet for workspace)
- Works in both "query" mode (export from database) and "workspace" mode (export from DuckDB)

## My Access Page

**Route**: `/my-access`

Self-service access view:
- Current SqlMode and what it allows
- Allowed connections list
- Database/table permission summary
- Storage permission summary
- Personal token management (generate, list, revoke)

## Approvals Page

**Route**: `/approvals`

Approval management:
- List pending approvals (own + those you can approve)
- View full SQL for each pending approval
- Approve or reject with reason
- Real-time SSE updates for new approvals
- See [[approvals]] for details

## Admin Page

**Route**: `/admin` (admin only)

Full administration panel with tabs:

### Users Tab
- Create, edit, delete users
- Set SqlMode, is_admin, is_enabled, mcp_enabled
- Set password, display name, phone

### Permissions Tab
- Select user → manage all permission types:
  - **Database permissions**: database + table pattern + R/W/U/D toggles
  - **Connection permissions**: whitelist of allowed connections
  - **Storage permissions**: connection + bucket pattern + R/W/D toggles

### Service Accounts Tab
- Create, edit, delete service accounts
- Set SqlMode, permissions, connection whitelist, storage permissions
- Rotate API keys

### PII Tab
- Manage [[pii]] rules (built-in + custom)
- Tag columns as PII
- Test rules against sample text

### Teams Tab
- Create and manage [[teams]]
- Add/remove members with role assignment
- Create projects within teams
- Configure webhook URLs

### Token Policy Tab
- System-wide token expiry settings
