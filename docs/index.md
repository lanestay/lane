# Lane Documentation

Lane is a self-contained database platform — query engine, REST API, MCP server, and React admin console — shipped as a single Rust binary. One process, no external dependencies, deploys anywhere.

## What it does

**Query any database from one place.** Connect SQL Server, PostgreSQL, and DuckDB instances by name. Run queries through the REST API, the MCP server (for AI agents), or the built-in web UI. Switch between connections and databases without reconfiguring anything.

**Turn queries into APIs.** Save any SQL query as a named endpoint with `{{parameters}}`. Consumers call it by name — no SQL knowledge needed, no direct database access required. Supports JSON and streaming NDJSON with configurable row limits.

**Control who sees what.** Four SQL permission tiers (None, Read Only, Supervised, Full), per-table CRUD permissions, per-connection access restrictions, and service accounts for automation. Supervised mode requires admin approval before writes execute.

**Protect sensitive data automatically.** Built-in PII detection that scrubs emails, SSNs, credit cards, and phone numbers before they leave the server. Tag columns, define custom regex rules, or let the auto-detector handle it.

**Browse and integrate object storage.** Connect MinIO or S3-compatible storage. Browse buckets, upload files, preview content, export query results to storage, import files into the workspace, and export workspace data back — all through the same API, MCP tools, and UI.

**Stream changes in real time.** Enable Server-Sent Events on any table. When a write goes through lane, subscribers get notified instantly — no polling, no external broker.

**Give AI agents safe database access.** The MCP server exposes 33 tools with read/write separation. Read-only tools are safe to auto-approve; write tools are marked destructive so MCP clients can prompt before executing. Agents with Full SQL mode can operate autonomously, while Supervised and Confirmed modes route writes through human approval. AI agents get structured schema discovery, query validation, and the same permission model as human users.

## Getting Started

- [[setup]] — Installation, configuration, and first-run setup
- [[connections]] — Database and storage connection configuration

## Core Features

- [[query-engine]] — SQL execution, validation, pagination, and row limits
- [[endpoints]] — Named, parameterized data endpoints (no-SQL API access)
- [[search]] — Full-text search across schema, queries, and endpoints (FTS5)
- [[storage]] — MinIO/S3 object storage browsing, upload, and preview
- [[workspace]] — DuckDB workspace for import, export, and analysis
- [[realtime]] — Live query monitoring and event streaming

## Security & Access Control

- [[auth]] — Authentication methods (API keys, tokens, sessions)
- [[permissions]] — SqlMode tiers, database/table/connection/storage permissions
- [[approvals]] — Supervised and confirmed write workflows
- [[pii]] — PII detection, redaction, column tagging, and custom rules
- [[teams]] — Teams, projects, roles, and approval delegation

## Interfaces

- [[ui]] — React UI pages and features
- [[api]] — REST API reference (all endpoints)
- [[mcp]] — MCP server tools reference (33 tools)
