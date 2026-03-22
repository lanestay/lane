# ── Stage 1: Build the React UI ──────────────────────────────────────────────
FROM node:24-slim AS ui-builder
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# ── Stage 2: Build the Rust binary ───────────────────────────────────────────
FROM rust:1.94-trixie AS builder
RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY --from=ui-builder /app/ui/dist/ ui/dist/
RUN cargo build --release --features webui,postgres,duckdb_backend,clickhouse_backend,storage

# ── Stage 3: Minimal runtime image ──────────────────────────────────────────
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/lane /usr/local/bin/lane

WORKDIR /app
VOLUME /app/data
EXPOSE 3401

# Inside the container the app must bind 0.0.0.0 so Docker networking works.
# Host-side exposure is controlled by the ports mapping in docker-compose.yml.
ENV HOST=0.0.0.0
ENV PORT=3401
ENV LANE_DATA_DIR=/app/data

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:3401/health || exit 1

ENTRYPOINT ["lane"]
