# Happy Server Deployment Guide

## Overview

Deployed to standalone node `sn208133` via Drone CI.

- **Branch:** `master` (prod)
- **Domain:** server.boujot.com
- **Registry:** registry.sn1994.zivili.ch/meow/happy

## Stack

| Component | Details |
|-----------|---------|
| Runtime | Node.js 20 |
| Database | PostgreSQL 16 (local container) |
| Cache | Redis 7 |
| Object Storage | MinIO (S3-compatible) |
| Port | 3000 |

## Initial Setup (one-time)

### 1. Create Vault AppRole

```bash
cd /Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-platform

OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES ansible-playbook playbooks/46_vault_project_approle.yml \
  -e repo_name=happy \
  -e repo_path=/Volumes/AppleFS/kDrive/Documents/workspace/happy \
  -e 'repo_targets=["sn208133"]' \
  -e deploy_key_scope=standalone-sn208133
```

### 2. Push secrets to Vault

```bash
./scripts/vault-env-push sn1994 /Volumes/AppleFS/kDrive/Documents/workspace/happy/.env.prod
```

### 3. Setup Gitea repo

```bash
./scripts/gitea-setup-repo.sh happy /Volumes/AppleFS/kDrive/Documents/workspace/happy meow master
```

### 4. Setup Drone CI

```bash
# IMPORTANT: Export ANSIBLE_INFRA_OPS_PATH before running
export ANSIBLE_INFRA_OPS_PATH=/Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-infra-ops

# drone-setup-repo.sh takes only 2 arguments: repo_name and owner
# Do NOT pass a third argument (cluster name) - it's implicit
./scripts/drone-setup-repo.sh happy meow
```

> **Gotcha:** The script signature is `drone-setup-repo.sh <repo> <owner>`, NOT `<repo> <owner> <cluster>`. The sn1994 cluster is the default and doesn't need to be specified.

### 5. Update DNS

```bash
./scripts/dns-api.sh set zivili.ch sn208133 --record happy --make-unique
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

Realtime reliability releases that introduce the message outbox, Pub/Sub rooms,
or runtime-incarnation fencing are not compatible with old server pods. Stop
and drain every old writer first, apply the additive Prisma migrations while no
server is accepting traffic, and only then start the protocol-v1 server. Do not
run a mixed-version realtime fleet or allow old-writer traffic after migration.

Once a protocol-v1 server has accepted runtime traffic or written runtime-lease
state, do not restart an older binary against that database. Recover by rolling
forward to another v1-compatible build. If an old-binary downgrade is
unavoidable, first stop and drain every runtime and treat the downgrade as a
maintenance operation. The deploy engine may restore the previous image when a
configured health check fails, so automatic health-check rollback remains
disabled for this rollout. Verify HTTP health, the v1 lease response, and one
Socket.IO runtime round trip manually; recover from any failure by rolling
forward.

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
  | tee /tmp/happy-v1-session.json \
  | jq -e '.session.runtimeConnectionProtocolVersion == 1
           and .session.runtimeConnected == true
           and (.session.runtimeLeaseExpiresAt | type == "number")
           and (.session.runtimeConnectionCheckedAt | type == "number")'

boujot daemon status

ssh g@sn208133 \
  "cd /home/drone/apps/happy-server && docker compose ps && docker compose logs --since=5m app"
```

Finally, send one uniquely tagged read-only prompt to that session from Agent
Plane and require its response to appear without refresh or recovery controls.
Re-run the authenticated session query and require `runtimeConnected == true`.
Any failed check is a roll-forward condition; do not restart the pre-v1 image.

## Updating Secrets

```bash
cd /Volumes/AppleFS/kDrive/Documents/workspace/ansible/ansible-platform

# Edit .env.prod locally, then push to Vault
./scripts/vault-env-push sn1994 /Volumes/AppleFS/kDrive/Documents/workspace/happy/.env.prod

# Redeploy to pick up new secrets
cd /Volumes/AppleFS/kDrive/Documents/workspace/happy
git commit --allow-empty -m "Trigger redeploy for secret update"
git push sn1994 master
```

## Monitoring

- Drone CI: https://drone.sn1994.zivili.ch/meow/happy
- Check containers: `ssh g@sn208133 "docker ps | grep happy"`
- App logs: `ssh g@sn208133 "docker logs happy_app_1"`

## Lessons Learned

### drone-setup-repo.sh syntax

**Wrong:**
```bash
./scripts/drone-setup-repo.sh happy meow sn1994  # 3 args - INCORRECT
```

**Correct:**
```bash
export ANSIBLE_INFRA_OPS_PATH=/path/to/ansible-infra-ops
./scripts/drone-setup-repo.sh happy meow  # 2 args only
```

The script requires:
1. `ANSIBLE_INFRA_OPS_PATH` environment variable set (for Docker registry credentials when macOS keychain is used)
2. Only 2 positional arguments: `<repo_name>` and `<owner>`

The cluster (sn1994) is implicit - it's the only Gitea/Drone instance.
