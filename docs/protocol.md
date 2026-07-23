# Protocol

This document describes the Happy wire protocol implemented in this repository.
The protocol is intentionally small: JSON over HTTP for reads/actions and
Socket.IO for real-time sync. Most payloads are end-to-end encrypted
client-side; see `encryption.md` for the encryption boundaries and encoding
details. For the full HTTP surface and auth flows, see `api.md`.

## Transport and versioning
- HTTP API: JSON requests/responses on `/v1` and `/v2` routes.
- WebSocket: Socket.IO server at path `/v1/updates` (transports: websocket, polling).
- CORS origin: `*`. HTTP preflight methods are currently limited to `GET`,
  `POST`, and `DELETE`; the legacy access-key `PUT` route is therefore not
  callable cross-origin from a browser. Socket.IO allows `GET`, `POST`, and
  `OPTIONS`.

## Protocol design motivations
The protocol is designed to stay minimal, explicit, and resilient under intermittent connectivity. A few guiding principles shape naming, payloads, and versioning:

- **Small surface area over completeness.** Routes and events exist only when they provide a clear sync primitive (e.g., sessions, artifacts, KV). If a capability can be expressed as data within an existing primitive, it should be.
- **Explicit event types and short keys.** Update payloads use `t` for the event type and concise field names (`sid`, `id`, `seq`) to keep message size down without hiding meaning. These names are stable because they are used across clients.
- **Separation of persistent vs. ephemeral.** Anything that must be recoverable after reconnect is an `update` event with a sequence number. Presence and usage are `ephemeral` to avoid state confusion and minimize storage.
- **Monotonic ordering at the user level.** `UpdatePayload.seq` is a single per-user counter. This makes client reconciliation simple: apply updates in order and you are consistent for that user.
- **Optimistic concurrency by default.** Versioned fields (metadata, agent state, artifact parts, access keys, KV) require `expectedVersion`. This prevents silent overwrites and keeps conflict resolution client-driven.
- **Client-side encryption boundaries.** The server never needs to understand plaintext. The protocol therefore treats most payloads as opaque strings or base64 blobs, which keeps server logic simple and privacy guarantees strong.
- **Backward compatibility over breaking changes.** New routes/events are added rather than mutating existing shapes in incompatible ways. When dual behavior is needed (e.g., machines), the server emits both old and new updates.
- **Avoid full REST verbs.** Reads are primarily `GET`, while writes/actions are primarily `POST`, with `DELETE` used when the intent is unambiguous. We avoid the full REST palette because many mutations are not cleanly tied to a single entity or involve more than CRUD logic. Keeping to `GET` + `POST` (plus occasional `DELETE`) makes the client simpler and the protocol clearer.

If a new protocol field or event is proposed, it should answer: does this create a durable sync primitive, or can it be encoded inside existing encrypted payloads without expanding the API surface?

## Authentication
Most endpoints require `Authorization: Bearer <token>`. The same token is also used in the Socket.IO handshake. Full auth flows and endpoints are documented in `api.md`.

### Current socket trust boundary

The bearer token binds the connection to an account, not to a cryptographic
client role. `clientType`, `sessionId`, `machineId`, `sessionInstanceId`, and
`replayOnly` are supplied by the caller. The server validates required
identifiers, account ownership where resources are loaded, and exact runtime
lease tuples for fenced session operations; it also forbids a session-scoped
socket from initiating `rpc-call`. Those checks do not make the role itself
credential-bound. Any process holding the account bearer must therefore be
treated as account-trusted. Separate least-privilege runtime/controller
credentials remain required before hostile-runtime isolation can be claimed.

## WebSocket connection
### Handshake
Connect with Socket.IO using:

```
path: "/v1/updates"
auth: {
  token: "<bearer token>",
  clientType: "user-scoped" | "session-scoped" | "machine-scoped",
  sessionId?: "<session id>",
  sessionInstanceId?: "<runtime UUID>",
  replayOnly?: boolean,
  machineId?: "<machine id>"
}
```

Rules enforced server-side:
- `token` is required.
- `session-scoped` requires `sessionId`.
- Durable session runtimes supply a process-incarnation UUID
  `sessionInstanceId`. Before CONNECT, the server claims that incarnation and
  mints a separate, server-side lease ID for this socket generation.
- A reconnect using the same active incarnation rotates the socket lease and
  evicts only the displaced lease generation. A different incarnation can take
  over only when the current owner is inactive or expired; the displaced
  incarnation is then durably retired as `superseded`.
- `replayOnly: true` is reserved for daemon outbox recovery. It can convert the
  matching current incarnation into a terminal replay transport and displace
  that incarnation's live socket. It cannot supersede a different live
  incarnation. Replay transport may emit only `message`, `session-end`, and
  `ping`.
- A replay handshake may omit `sessionInstanceId`. In that case the server
  infers or creates an incarnation only after the current owner is inactive,
  expired, or beyond the untouched-legacy grace window; it cannot displace a
  currently live owner without an explicit matching incarnation.
- `machine-scoped` requires `machineId`.

### Connection types
- `user-scoped`: receives account-wide updates.
- `session-scoped`: receives updates for a specific session only.
- `machine-scoped`: used by daemons; receives machine updates and emits machine state.

### Runtime lease and retirement semantics

- PostgreSQL time is authoritative. A socket lease lasts four minutes and is
  renewed by a runtime heartbeat expected at least once per minute; client
  timestamps never extend it.
- A live runtime packet is authorized against the exact tuple of account,
  session, process incarnation, and socket lease. Partial lease tuples fail
  closed. Metadata, state, messages, usage, RPC registration, and RPC delivery
  are fenced; message persistence repeats the check under the session lock.
- Disconnect rotates the exact lease to an expired tombstone. If disconnect
  cleanup is lost, the persisted expiry provides the bounded fallback.
- Process incarnations are single-use. Retirement rows are retained with status
  `replaying`, `ended`, or `superseded`, and a normally retired incarnation
  cannot become live again. A replay connection is marked `replaying`; durable
  `session-end` changes it to `ended`; a later process takeover can change a
  displaced replay incarnation to `superseded`.
- `session-end` belongs to the process incarnation rather than one transient
  socket lease, which lets a terminal replay finish after the original socket
  is gone without allowing it to end a newer process incarnation.
- Untouched pre-v1 rows have a bounded four-minute compatibility path. Once any
  lease marker is written, readiness and authorization never fall back to the
  legacy heuristic.

### Server -> client sync envelopes

The synchronization layer emits two envelope event names, `update` and
`ephemeral`. These are not the complete server-to-client Socket.IO event set:
RPC registration and dispatch use the separate control events documented below.

#### `update`
Persistent sync events. Payload shape:
```
{
  id: string,
  seq: number,
  body: { t: string, ... },
  createdAt: number
}
```

#### `ephemeral`
Transient presence/usage events. Payload shape:
```
{
  type: string,
  ...
}
```

### Update event types
Field names below match on-wire payloads.

- `new-session`
  - `body`: `{ t: "new-session", id, seq, metadata, metadataVersion, agentState, agentStateVersion, dataEncryptionKey, active, activeAt, createdAt, updatedAt }`

- `update-session`
  - `body`: `{ t: "update-session", id, metadata?, agentState? }`
  - `metadata`: `{ value, version }` or null
  - `agentState`: `{ value, version }` or null

- `delete-session`
  - `body`: `{ t: "delete-session", sid }`

- `new-message`
  - `body`: `{ t: "new-message", sid, message: { id, seq, content, localId, createdAt, updatedAt } }`

- `update-account`
  - `body`: `{ t: "update-account", id, settings?, github? }`

- `new-machine`
  - `body`: `{ t: "new-machine", machineId, seq, metadata, metadataVersion, daemonState, daemonStateVersion, dataEncryptionKey, active, activeAt, createdAt, updatedAt }`

- `update-machine`
  - `body`: `{ t: "update-machine", machineId, metadata?, daemonState?, activeAt? }`

- `new-artifact`
  - `body`: `{ t: "new-artifact", artifactId, seq, header, headerVersion, body, bodyVersion, dataEncryptionKey, createdAt, updatedAt }`

- `update-artifact`
  - `body`: `{ t: "update-artifact", artifactId, header?, body? }`

- `delete-artifact`
  - `body`: `{ t: "delete-artifact", artifactId }`

- `relationship-updated`
  - `body`: `{ t: "relationship-updated", uid, status, timestamp }`

- `new-feed-post`
  - `body`: `{ t: "new-feed-post", id, body, cursor, createdAt }`

- `kv-batch-update`
  - `body`: `{ t: "kv-batch-update", changes: [{ key, value, version }] }`

### Ephemeral event types
- `activity`: `{ type: "activity", id: sessionId, active, activeAt, thinking? }`
- `machine-activity`: `{ type: "machine-activity", id: machineId, active, activeAt }`
- `usage`: `{ type: "usage", id: sessionId, key, tokens, cost, timestamp }`
- `machine-status`: `{ type: "machine-status", machineId, online, timestamp }`
  is reserved in the server event type and builder. No current source path
  calls that builder, so the server does not presently emit this event.

### Server -> client RPC control events

- `rpc-request`: `{ callId?, method, params }`, delivered to the selected RPC
  target with an acknowledgement callback carrying its response.
- `rpc-registered`: `{ method }`, confirming a successful `rpc-register`.
- `rpc-unregistered`: `{ method }`, confirming a successful `rpc-unregister`.
- `rpc-error`: `{ type: "register" | "unregister", error }`, reporting a
  registration-control validation or internal failure.

### Client -> server WebSocket events
- `ping` -> callback `{}`

- `update-metadata`
  - `{ sid, metadata, expectedVersion }`
  - Response: `{ result: "success", version, metadata }` or `{ result: "version-mismatch", version, metadata }`

- `update-state`
  - `{ sid, agentState, expectedVersion }`
  - Response: `{ result: "success", version, agentState }` or `{ result: "version-mismatch", version, agentState }`

- `message`
  - `{ sid, message, localId? }`
  - Optional acknowledgement after the database commit: `{ result: "success", duplicate, message: { id, seq, localId, createdAt, updatedAt } }`.
  - Errors use `{ result: "error", code, retryable }`; reusing a `(sid, localId)` with different ciphertext returns `code: "idempotency_conflict"`.
  - A supplied `localId` must be a canonical 36-character UUID. Retrying identical ciphertext with the same UUID is idempotent. Producers without a callback or `localId` remain supported but do not get durable retry semantics.
  - The acknowledgement means the message and its notification outbox row
    committed together. The outbox snapshots the exact live runtime socket
    lease, or the narrowly bounded legacy target, in the same transaction.
    Its live notification is not retargeted to a later runtime generation.
  - Outbox dispatch polls each second and claims at most 25 session heads per
    batch. It is FIFO within each session. Workers use a 30-second claim, retry
    failed publications with jittered backoff from 500 ms to 60 seconds, and
    retain delivered rows for 24 hours before hourly cleanup. Undelivered rows
    remain eligible for retry.
  - Redis Pub/Sub is only a best-effort live wakeup; PostgreSQL is canonical.
    Live delivery is at-least-once and may duplicate. Consumers must deduplicate
    by update/message id and reconcile with the cursor API on startup,
    reconnect, and sequence gaps. The message cursor, not delivered outbox-row
    retention, is the long-term recovery source.
  - Encrypted ciphertext is limited to 7 MiB per event.

- `session-alive`
  - `{ sid, time, thinking? }`
  - Emits `ephemeral` activity to user-scoped connections.

- `session-end`
  - Legacy form: `{ sid, time }` (no durable acknowledgement).
  - Durable form: `{ sid, time, localId, sessionInstanceId }`, where both IDs
    are canonical UUIDs and `time` is the persisted marker creation time.
  - Success acknowledgement after the inactive state commits:
    `{ result: "success", localId }`. Errors use
    `{ result: "error", code, retryable }`.
  - The server applies a durable end only to its originating active runtime
    instance. ACK-loss retries are successful no-ops after the first commit,
    and an old marker cannot terminate a later resumed instance.

- `usage-report`
  - `{ key, sessionId?, tokens, cost }`
  - Stores usage report and optionally emits `ephemeral` usage for the session.

- `machine-alive`
  - `{ machineId, time }`
  - Emits `ephemeral` machine-activity.

- `machine-update-metadata`
  - `{ machineId, metadata, expectedVersion }`
  - Response: `{ result: "success", version, metadata }` or `{ result: "version-mismatch", version, metadata }`

- `machine-update-state`
  - `{ machineId, daemonState, expectedVersion }`
  - Response: `{ result: "success", version, daemonState }` or `{ result: "version-mismatch", version, daemonState }`

- `artifact-read`
  - `{ artifactId }`
  - Response: `{ result: "success", artifact }` or `{ result: "error", message }`

- `artifact-create`
  - `{ id, header, body, dataEncryptionKey }`
  - Response: `{ result: "success", artifact }` or `{ result: "error", message }`

- `artifact-update`
  - `{ artifactId, header?, body? }` where `header` and `body` include `data` + `expectedVersion`
  - Response: `{ result: "success", header?, body? }` or `{ result: "version-mismatch", header?, body? }`

- `artifact-delete`
  - `{ artifactId }`
  - Response: `{ result: "success" }` or `{ result: "error", message }`

- `access-key-get`
  - `{ sessionId, machineId }`
  - Response: `{ ok: true, accessKey? }` or `{ ok: false, error }`

- `rpc-register`
  - `{ method }` -> server emits `rpc-registered`

- `rpc-unregister`
  - `{ method }` -> server emits `rpc-unregistered`

- `rpc-call`
  - `{ method, params, callId? }` -> callback `{ ok, result? | error?, callId? }`.
  - When present, `callId` is a UUID forwarded unchanged in `rpc-request` and echoed by the server.
  - Ownership-lock contention, or a known runtime registration whose exact
    local socket vanished before dispatch, returns
    `{ ok: false, outcome: "not_started", retryable: true, error, callId? }`.
    A caller may retry only this exact response, with a matching `callId`, while
    preserving the same `callId`, method, and serialized payload.
  - An absent registry entry, stale runtime ownership, invalid input, registry
    failure, or plain `{ ok: false, error }` response does not carry the
    `not_started` proof and must not be retried automatically.
  - After an attempted delivery, a timeout/disconnect returns `{ ok: false, outcome: "unknown", callId? }`; it never fails over, and callers must not blindly retry, because execution may already have happened.
  - Runtime-owned RPC is dispatched directly to the exact local socket under the database owner lock. The supported deployment is one server replica; multi-replica runtime RPC requires a receiver-side relay that reacquires and validates the exact runtime owner before local dispatch. Nonlocal runtime targets fail as `not_started`.
  - RPC parameters are limited to 4 MiB after serialization.

## HTTP endpoints by area
See `api.md` for the full HTTP endpoint catalog and auth flows.

## Sequencing and concurrency
- `UpdatePayload.seq` is the per-user update sequence (monotonic) used for sync ordering.
- Sessions, machines, and artifacts have their own `seq` fields used by clients for ordering.
- Versioned fields (metadata, agentState, daemonState, artifact header/body, access keys, KV) use optimistic concurrency with `expectedVersion` and return a version-mismatch response containing the current version/data.

## Realtime reliability configuration

- Runtime-owned RPC currently requires exactly one Happy server replica. Use
  Recreate deployment semantics; do not use rolling or horizontally scaled app
  writers until receiver-side runtime RPC fencing exists.
- `SOCKET_IO_REDIS_CHANNEL_PREFIX` (default `happy:socket.io`) selects the official Socket.IO Redis Pub/Sub adapter's channel prefix. Socket.IO connection-state recovery is disabled.
- `SESSION_MESSAGE_NOTIFICATION_REDIS_CHANNEL` (default `happy:session-message-notifications`) carries committed message wakeups to every server pod through Redis Pub/Sub.
- Redis contains only transient Pub/Sub traffic and small RPC registration
  values (`socketId`, sortable generation, and runtime-owner tuple where
  applicable). Registrations have a 60-second TTL, refresh every 20 seconds,
  and are capped at 256 methods per socket. Registry commands disable offline
  queuing and automatic resend.
- Redis is not message retention; PostgreSQL and the message cursor are the
  recovery path.
- Compose and Kubernetes configure AOF persistence, a volume, and a bounded `volatile-ttl` memory policy. Registry keys self-reclaim after Redis restarts and converge to the freshest connected generation.

## Implementation references
- API routes: `sources/app/api/routes`
- Socket handlers: `sources/app/api/socket`
- Runtime leases: `sources/app/presence/runtimeConnectionLease.ts`
- Durable message outbox: `sources/app/session/sessionMessageNotificationOutbox.ts`
- Event routing: `sources/app/events/eventRouter.ts`
