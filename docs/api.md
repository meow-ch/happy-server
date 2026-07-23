# API

This document covers the HTTP API surface and authentication flows. For WebSocket updates and event payloads, see `protocol.md`. For encryption boundaries and encoding details, see `encryption.md`.

## Method conventions
- **GET** is used for reads.
- **POST** is used for mutations or actions, even when the operation doesn't map cleanly to a single entity.
- **DELETE** is used when intent is unambiguous (e.g., removing a token or deleting a session/artifact).

We intentionally avoid the full REST verb palette because many operations span multiple entities or have non-CRUD semantics.

One legacy exception is the access-key `PUT` route listed below. The HTTP CORS
allowlist currently contains only `GET`, `POST`, and `DELETE`, so a
cross-origin browser preflight for that `PUT` route fails. Same-origin and
non-browser callers can use it; browser clients must not assume cross-origin
support until `PUT` is added to the server allowlist.

## Authentication
Most endpoints require `Authorization: Bearer <token>`.

Auth flows:
- `POST /v1/auth`
  - Body: `{ publicKey, challenge, signature }` (base64 strings)
  - Verifies signature using the provided public key.
  - Upserts account by public key and returns `{ success, token }`.

- `POST /v1/auth/request`
  - Body: `{ publicKey, supportsV2? }`
  - Creates or returns a terminal auth request.
  - Response: `{ state: "requested" }` or `{ state: "authorized", token, response }`.

- `GET /v1/auth/request/status?publicKey=...`
  - Response: `{ status: "not_found" | "pending" | "authorized", supportsV2 }`.

- `POST /v1/auth/response`
  - Body: `{ response, publicKey }` (requires Bearer auth)
  - Approves a terminal auth request.

- `POST /v1/auth/account/request`
  - Body: `{ publicKey }`
  - Similar to terminal auth, but for account linking.

- `POST /v1/auth/account/response`
  - Body: `{ response, publicKey }` (requires Bearer auth)

## Endpoint catalog
### Sessions
- `GET /v1/sessions`
- `GET /v2/sessions/active?limit=...`
- `GET /v2/sessions?cursor=cursor_v1_<id>&limit=...&changedSince=...`
- `POST /v1/sessions` (create or load by `tag`)
- `GET /v1/sessions/:sessionId` (authoritative protocol-v1 runtime connection
  snapshot; includes inactive and old sessions owned by the authenticated
  account)
- `GET /v1/sessions/:sessionId/messages` (legacy latest 150)
- `GET /v1/sessions/:sessionId/messages?afterSeq=...&limit=...` (canonical ascending cursor replay, up to 500 per page; use on startup, reconnect, and sequence gaps because Redis fanout is best-effort)
- `DELETE /v1/sessions/:sessionId`

The single-session response is:

```json
{
  "session": {
    "id": "<session id>",
    "agentStateVersion": 1,
    "runtimeConnectionProtocolVersion": 1,
    "runtimeConnected": true,
    "runtimeInstanceId": "<runtime process UUID or null>",
    "runtimeLeaseInstanceId": "<leased runtime process UUID or null>",
    "runtimeLeaseExpiresAt": 1784790000000,
    "runtimeInstanceRetired": false,
    "runtimeConnectionCheckedAt": 1784789900000,
    "dataEncryptionKey": "<base64 or null>"
  }
}
```

`runtimeConnectionCheckedAt` and `runtimeLeaseExpiresAt` are Unix milliseconds
from one PostgreSQL-clock snapshot. For a protocol-v1 managed runtime,
`runtimeConnected` is true only when the session is active, the active process
and lease instance match, the lease has not expired at the checked time, and
the process incarnation is not retired. A bounded compatibility heuristic is
used only for untouched legacy rows; once a lease marker exists, the row never
falls back to that heuristic. The response is `private, no-store`; an unknown
or other account's session returns `404` with `SESSION_NOT_FOUND`.

Cursor replay returns `{ messages, hasMore, nextAfterSeq }`. Continue until
`hasMore` is false and retain `nextAfterSeq`; live notifications are
best-effort, potentially duplicate hints, while this endpoint is the authoritative gap repair path.

### Machines
- `POST /v1/machines` (create or load by id)
- `GET /v1/machines`
- `GET /v1/machines/:id`

### Artifacts
- `GET /v1/artifacts`
- `GET /v1/artifacts/:id`
- `POST /v1/artifacts`
- `POST /v1/artifacts/:id` (versioned update)
- `DELETE /v1/artifacts/:id`

### Access keys
- `GET /v1/access-keys/:sessionId/:machineId`
- `POST /v1/access-keys/:sessionId/:machineId`
- `PUT /v1/access-keys/:sessionId/:machineId`

### Key-value store
- `GET /v1/kv/:key`
- `GET /v1/kv?prefix=...&limit=...`
- `POST /v1/kv/bulk`
- `POST /v1/kv` (batch mutate)

### Account and usage
- `GET /v1/account/profile`
- `GET /v1/account/settings`
- `POST /v1/account/settings`
- `POST /v1/usage/query`

### Push tokens
- `POST /v1/push-tokens`
- `DELETE /v1/push-tokens/:token`
- `GET /v1/push-tokens`

### Connect (GitHub + vendor tokens)
- `GET /v1/connect/github/params`
- `GET /v1/connect/github/callback`
- `POST /v1/connect/github/webhook`
- `DELETE /v1/connect/github`
- `POST /v1/connect/:vendor/register` (`vendor` in `openai | anthropic | gemini`)
- `GET /v1/connect/:vendor/token`
- `DELETE /v1/connect/:vendor`
- `GET /v1/connect/tokens`

### Users, friends, feed
- `GET /v1/user/:id`
- `GET /v1/user/search?query=...`
- `POST /v1/friends/add`
- `POST /v1/friends/remove`
- `GET /v1/friends`
- `GET /v1/feed`

### Version
- `POST /v1/version`

### Dev-only
- `POST /logs-combined-from-cli-and-mobile-for-simple-ai-debugging` (only if enabled)

## Implementation references
- API routes: `sources/app/api/routes`
- Auth module: `sources/app/auth/auth.ts`
