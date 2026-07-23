# Happy Server Deployment Guide

## Overview

Deployed to standalone node `sn208133` via Drone CI.

- **Branch:** `master` (prod)
- **Domain:** server.boujot.com
- **Registry:** registry.sn1994.zivili.ch/meow/happy-server

## Stack

| Component | Details |
|-----------|---------|
| Runtime | Node.js 20 |
| Database | PostgreSQL 16 (local container) |
| Cache | Redis 7 |
| Object Storage | MinIO (S3-compatible) |
| Application port | 3005 |

## Initial Setup (one-time)

### 1. Create Vault AppRole

```bash
cd /Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-platform

OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES ansible-playbook playbooks/46_vault_project_approle.yml \
  -e repo_name=happy-server \
  -e repo_path=/Volumes/AppleFS/kDrive/Documents/workspace/happy/happy-server \
  -e 'repo_targets=["sn208133"]' \
  -e deploy_key_scope=standalone-sn208133
```

### 2. Push secrets to Vault

```bash
./scripts/vault-env-push sn1994 /Volumes/AppleFS/kDrive/Documents/workspace/happy/happy-server/.env.prod
```

### 3. Setup Gitea repo

```bash
./scripts/gitea-setup-repo.sh happy-server /Volumes/AppleFS/kDrive/Documents/workspace/happy/happy-server meow master
```

### 4. Setup Drone CI

```bash
# IMPORTANT: Export ANSIBLE_INFRA_OPS_PATH before running
export ANSIBLE_INFRA_OPS_PATH=/Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-infra-ops

# drone-setup-repo.sh takes only 2 arguments: repo_name and owner
# Do NOT pass a third argument (cluster name) - it's implicit
./scripts/drone-setup-repo.sh happy-server meow
```

> **Gotcha:** The script signature is `drone-setup-repo.sh <repo> <owner>`, NOT `<repo> <owner> <cluster>`. The sn1994 cluster is the default and doesn't need to be specified.

### 5. Update DNS

```bash
./scripts/dns-api.sh set boujot.com sn208133 --record server --make-unique
```

### 6. Create external volumes on server

Postgres and MinIO volumes are `external: true` to prevent accidental deletion with `docker compose down -v`. Redis uses the Compose-managed named volume `happy_redis_data`, which is created automatically on the first deploy.

```bash
ssh g@sn208133 "docker volume create happy_postgres_data && docker volume create happy_minio_data"
```

## Deploying Changes

Push to `master` branch triggers automatic build and deploy:

```bash
git push sn1994 master
```

Production supports exactly one Happy app replica because runtime-owned RPC is
delivered to the exact local socket while a PostgreSQL owner lock is held. The
deployment contract is Recreate, not rolling replacement:

1. Ask the old app to stop and wait until Docker reports that it has exited.
   The 45-second termination grace is a hard outer limit, not a guarantee that
   every concurrent shutdown hook drained.
2. Confirm no old app writer remains.
3. Run `prisma migrate deploy` with no server accepting traffic.
4. Start exactly one new app process and complete the readiness checks below.

The image entrypoint performs steps 3 and 4 sequentially. The deploy engine must
fully stop the old Compose project before starting the new image. Do not use
`docker compose up --scale app=...`, a rolling replacement, or mixed-version
writers.

Realtime reliability releases that introduce the message outbox, Pub/Sub rooms,
or runtime-incarnation fencing are not compatible with old server pods. Do not
allow old-writer traffic after migration.

Once a protocol-v1 server has accepted runtime traffic or written runtime-lease
state, do not restart an older binary against that database. Draining runtimes
does not make the schema or existing outbox rows downgrade-safe. Recover by
rolling forward to another v1-compatible build. An old-binary downgrade is
prohibited unless a separately reviewed database schema/data downgrade and
recovery plan has been executed. The deploy engine may restore the previous
image when a configured health check fails, so `deploy.yml` sets
`healthcheck_exempt: true`.
Keep automatic rollback disabled while an available rollback image could be
lease-unaware. Verify HTTP health, the v1 lease response, and one Socket.IO
runtime round trip manually; recover from any failure by rolling forward.

### Protocol-v1 smoke check

Use a real authenticated session/runtime; do not substitute a database edit for
the Socket.IO check.

```bash
export HAPPY_SMOKE_TOKEN='<bearer token for the test account>'
export HAPPY_SMOKE_SESSION_ID='<live Boujot session id>'

curl -fsS https://server.boujot.com/health \
  | jq -e '.status == "ok"'

curl -fsS \
  -H "Authorization: Bearer ${HAPPY_SMOKE_TOKEN}" \
  "https://server.boujot.com/v1/sessions/${HAPPY_SMOKE_SESSION_ID}" \
  | jq -e '.session as $s
           | $s.runtimeConnectionProtocolVersion == 1
           and $s.runtimeConnected == true
           and ($s.runtimeInstanceId | type == "string")
           and $s.runtimeLeaseInstanceId == $s.runtimeInstanceId
           and $s.runtimeInstanceRetired == false
           and ($s.runtimeLeaseExpiresAt | type == "number")
           and ($s.runtimeConnectionCheckedAt | type == "number")
           and $s.runtimeLeaseExpiresAt > $s.runtimeConnectionCheckedAt'

boujot daemon status

ssh g@sn208133 \
  "cd /home/drone/apps/happy-server && docker compose ps && docker compose logs --since=5m app"
```

Use an isolated smoke session to perform one uniquely tagged, read-only
Socket.IO/RPC round trip. Preserve one UUID `callId`; do not retry unless Happy
returns the exact `not_started` retry contract documented in
`docs/protocol.md`. Require the response to appear without refresh or recovery
controls, then re-run the authenticated session query and require
`runtimeConnected == true`.

Soak the exact deployed image for at least six minutes, then verify:

- the app image tag equals the intended Git commit;
- restart count is zero and `OOMKilled` is false;
- public and direct-container `/health` return 200;
- logs since container start contain no `MaxListenersExceededWarning`,
  `ERROR`, `FATAL`, `PANIC`, `Unhandled`, or `Exception`; and
- the protocol-v1 migration remains applied without rollback.

Any failed check is a roll-forward condition; do not restart the pre-v1 image.

The deploy engine currently recreates the entire Compose project, including
Postgres, Redis, and MinIO. `depends_on` does not wait for dependency health.
The app may initially fail its migration-first entrypoint and restart until
Postgres is ready. Treat this as a full-project outage and verify the eventual
healthy exact image; do not add an automatic old-image rollback.

## Updating Secrets

The procedure below applies only to secrets that are already propagated by
`docker-compose.yml`. Adding a value to `.env.prod` does not inject it into the
app unless the Compose environment allowlist contains that variable.

Do **not** rotate `HANDY_MASTER_SECRET` with this generic procedure. It derives
bearer/GitHub token generators and the KeyTree used to encrypt stored
GitHub/vendor service tokens. The repository has no key-version or automatic
re-encryption migration, so blind rotation can invalidate authentication and
make stored service tokens undecryptable. A compromise requires a separately
reviewed key-versioning, re-encryption, token invalidation, and re-authentication
plan before changing the value.

```bash
cd /Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-platform

# Edit .env.prod locally, then push to Vault
./scripts/vault-env-push sn1994 /Volumes/AppleFS/kDrive/Documents/workspace/happy/happy-server/.env.prod

# Redeploy to pick up new secrets
cd /Volumes/AppleFS/kDrive/Documents/workspace/happy/happy-server
git commit --allow-empty -m "Trigger redeploy for secret update"
git push sn1994 master
```

GitHub, metrics override, and dangerous debug variables are not currently
passed by the production Compose file. Pushing those values to Vault does not
activate them. GitHub callback completion also targets the upstream Happy app;
the integration is unavailable in this fork's production deployment until
both issues are corrected.

## Monitoring

- Drone CI: https://drone.sn1994.zivili.ch/meow/happy-server
- Check containers: `ssh g@sn208133 "docker ps | grep happy"`
- App logs: `ssh g@sn208133 "docker logs happy-server-app-1"`

## Lessons Learned

### drone-setup-repo.sh syntax

**Wrong:**
```bash
./scripts/drone-setup-repo.sh happy-server meow sn1994  # 3 args - INCORRECT
```

**Correct:**
```bash
export ANSIBLE_INFRA_OPS_PATH=/path/to/ansible-infra-ops
./scripts/drone-setup-repo.sh happy-server meow  # 2 args only
```

The script requires:
1. `ANSIBLE_INFRA_OPS_PATH` environment variable set (for Docker registry credentials when macOS keychain is used)
2. Only 2 positional arguments: `<repo_name>` and `<owner>`

The cluster (sn1994) is implicit - it's the only Gitea/Drone instance.
