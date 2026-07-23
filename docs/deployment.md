# Deployment

This document describes how to deploy this Happy server repository and the
infrastructure it expects. Production-specific host, registry, and smoke-test
commands are in `../DEPLOY.md`.

## Runtime overview
- **App server:** Node.js running `prisma migrate deploy` and then
  `tsx ./sources/main.ts` as PID 1 (Fastify + Socket.IO).
- **Database:** Postgres via Prisma.
- **Realtime bus:** Redis for the Socket.IO adapter, committed-message wakeups,
  and TTL-bound RPC registrations. PostgreSQL remains canonical.
- **Object storage:** S3-compatible storage for user-uploaded assets (MinIO works).
- **Metrics:** Optional Prometheus `/metrics` server on a separate port.

## Required services
1. **Postgres**
   - Required for all persisted data.
   - Configure via `DATABASE_URL`.

2. **Redis**
   - Required by startup (`redis.ping()` is called).
   - Configure via `REDIS_URL`.

3. **S3-compatible storage**
   - Used for avatars and other uploaded assets.
   - Configure via `S3_HOST`, `S3_PORT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_PUBLIC_URL`, `S3_USE_SSL`.

## Environment variables

The production Compose file passes only the variables listed in its
`services.app.environment` block. Values present in `.env.prod` or Vault but
absent from that allowlist are used only for Compose substitution and are not
visible to the Node process.

**Required and propagated by production Compose**
- `DATABASE_URL`: Postgres connection string.
- `HANDY_MASTER_SECRET`: master key for auth tokens and server-side encryption.
- `REDIS_URL`: Redis connection string.
- `S3_HOST`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_PUBLIC_URL`: object storage config.

**Propagated configuration**
- `PORT`: API server port (default `3005`).
- `SOCKET_IO_REDIS_CHANNEL_PREFIX`: Socket.IO adapter channel prefix.
- `SESSION_MESSAGE_NOTIFICATION_REDIS_CHANNEL`: committed-message wakeup channel.
- `S3_PORT`: optional S3 port.
- `S3_USE_SSL`: `true`/`false` (default `true`).

The source also reads `METRICS_ENABLED`, `METRICS_PORT`, GitHub credentials and
redirect settings, and
`DANGEROUSLY_LOG_TO_SERVER_FOR_AI_AUTO_DEBUGGING`. The current production
Compose file does **not** pass those variables. Metrics therefore use their
source defaults, and GitHub/debug configuration in `.env.prod` has no effect.
GitHub callback completion also hard-codes the upstream Happy application URL,
so GitHub connect is not production-ready for this fork even if the variables
are added.

## Docker image
A production image is provided by the repository-root `Dockerfile`.

Key notes:
- The server defaults to port `3005` (set `PORT` explicitly in container environments).
- The image includes FFmpeg and Python for media processing.
- The entrypoint applies pending Prisma migrations before it starts the API and
  uses `exec` so Docker signals reach the application shutdown handlers.

## Deployment safety contract

Runtime-owned RPC currently supports exactly one Happy app replica. It delivers
to an exact local runtime socket while holding the PostgreSQL owner fence;
multi-replica runtime RPC requires a receiver-side relay that reacquires and
validates that fence.

Deploy with Recreate semantics:

1. Ask every old app writer to stop and wait until Docker reports it stopped.
2. Confirm no old app process is accepting traffic.
3. Apply migrations with no server writer running.
4. Start exactly one new app process.
5. Manually verify HTTP health, the authenticated protocol-v1 session lease,
   and an isolated read-only Socket.IO/RPC round trip.

Never run lease-aware and lease-unaware writers concurrently. Once a
protocol-v1 server has accepted runtime traffic or written lease state, do not
automatically roll back to an older binary; fix forward with a compatible
image. The production deploy therefore uses `healthcheck_exempt: true` and
manual readiness. The current shutdown hooks run concurrently without a global
timeout or phase ordering. The 45-second Docker grace is an outer limit, not
proof that sockets, RPC registrations, the outbox dispatcher, Redis, and
Prisma drained cleanly. Confirm process termination before migration and rely
on durable database state—not shutdown completion logs—for recovery.

The production deploy engine performs `docker compose down` followed by
`docker compose up`, so Postgres, Redis, and MinIO restart as part of the same
outage. Compose `depends_on` orders starts but has no dependency health
conditions. The app restart policy reruns its migration-first entrypoint if a
dependency is not ready; operators must expect transient startup failures and
verify eventual readiness from the exact image rather than rolling back.

After readiness, soak the exact image for at least six minutes. Require zero
restarts, no OOM kill, healthy public and direct-container endpoints, and no
`MaxListenersExceededWarning`, fatal, panic, unhandled, or exception log lines.

## Kubernetes manifests

The files under `deploy` are retained upstream/legacy examples, not runnable
production manifests for the Boujot deployment:

- `handy.yaml`: uses upstream names, registry/domain placeholders, and legacy
  secret paths. Do not apply it to Boujot production.
- `happy-redis.yaml`: Redis StatefulSet + Service + ConfigMap.

If adapting the legacy manifest, preserve these current constraints:
- `strategy.type: Recreate` and `replicas: 1` for the Happy app.
- A 45-second termination grace period.
- Prometheus scraping annotations on port `9090`.
- A secret named `handy-secrets` populated by ExternalSecrets.
- A service mapping port `3000` to container port `3005`.

## Local dev helpers
The server package includes scripts for local infrastructure:
- `yarn db` (Postgres in Docker)
- `yarn redis`
- `yarn s3` + `yarn s3:init`

Use `.env`/`.env.dev` to load local settings when running `yarn dev`.

## Implementation references
- Entrypoint: `sources/main.ts`
- Dockerfile: `Dockerfile`
- Compose file: `docker-compose.yml`
- Production deploy declaration: `deploy.yml`
- Kubernetes manifests: `deploy`
- Env usage: `sources` (`rg -n "process.env"`)
