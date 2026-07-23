# Known Limitations

Status: active
Last reviewed: 2026-07-23

This file records boundaries that the current code and production deployment
do not yet close. It is part of the operational contract, not a wishlist that
may be ignored during release review.

## Shutdown is not a bounded phased drain

`sources/utils/shutdown.ts` aborts one shared signal and starts all registered
handlers concurrently. It has no global timeout or ordering between API,
Socket.IO/RPC, Redis, storage, and `keepAlive` handlers. The presence timeout
loop also uses an unconditional inner loop; after abort its delay returns
immediately, so it can re-enter database work until Prisma disconnect causes
it to fail.

Docker's 45-second stop grace is the only hard outer bound and may end with a
forced stop. Deployment must confirm that the old process is gone before
migration, and recovery must rely on durable Postgres state. A complete fix
requires a shutdown-aware presence loop plus explicit, globally bounded
shutdown phases.

## Production is one Happy application replica

Runtime RPC dispatch is process-local while a PostgreSQL owner fence is held.
There is no cross-replica receiver that reacquires that fence. Recreate one
application replica; do not roll or scale horizontally.

## Push delivery is client-owned and not durable

The server exposes authenticated create/list/delete operations for account push
tokens, but it has no notification sender, queue, delivery outbox, receipt
processor, or retry worker. The sibling Happy CLI currently fetches those tokens
and sends title, body, and data directly to Expo in a detached best-effort task.
CLI process exit, network loss, or exhausted retries can therefore lose a
notification.

Push title/body/data are not Happy conversation ciphertext and are visible to
the external push service. Do not describe the push payload as end-to-end
encrypted, and do not use push delivery as an authoritative completion or
permission signal.

## Production Compose recreates dependencies without health gates

The current deploy engine runs full `docker compose down` and `up`, restarting
Postgres, Redis, and MinIO. Compose `depends_on` supplies start order only.
There are no dependency health conditions, so the migration-first app
entrypoint can fail and restart while Postgres becomes ready.

## Socket roles are caller-asserted

The bearer token authenticates an account. It does not cryptographically bind
`clientType`, `sessionId`, `machineId`, or `replayOnly`. Resource ownership and
runtime lease checks fence sensitive operations, but a process holding the
account bearer remains account-trusted. Runtime and controller credentials are
not least-privilege separated.

## Production environment propagation is allowlisted

Compose passes only variables named in `services.app.environment`. GitHub,
metrics override, and dangerous debug variables read by source code are not
currently passed. GitHub callback completion also redirects to the upstream
Happy application. GitHub connect is therefore unavailable in this fork's
production deployment.

## Cross-origin browser `PUT` is not enabled

The access-key API has a `PUT` route, but Fastify CORS allows only `GET`,
`POST`, and `DELETE`. A cross-origin browser preflight for `PUT` fails.

## The master secret has no rotation protocol

`HANDY_MASTER_SECRET` derives authentication token generators and at-rest
service-token encryption. There is no key versioning or re-encryption
migration. It must not be changed through generic secret rotation.

## Rollback is roll-forward only after protocol-v1 writes

Older lease-unaware binaries are not schema/data compatible merely because
runtimes were drained. Once protocol-v1 state exists, recovery uses a
v1-compatible forward build unless a separately reviewed database downgrade
and data-recovery procedure is executed.
