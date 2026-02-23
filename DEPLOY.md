# Happy Server Deployment Guide

## Overview

Deployed to standalone node (ov8374) via Drone CI.

- **Branch:** `master` (prod)
- **Domain:** happy.zivili.ch
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
  -e 'repo_targets=["ov8374"]' \
  -e deploy_key_scope=standalone
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
./scripts/dns-api.sh set zivili.ch ov8374 --record happy --make-unique
```

### 6. Create external volumes on server

Volumes are `external: true` to prevent accidental deletion with `docker compose down -v`.

```bash
ssh g@ov8374 "docker volume create happy_postgres_data && docker volume create happy_minio_data"
```

## Deploying Changes

Push to `master` branch triggers automatic build and deploy:

```bash
git push sn1994 master
```

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
- Check containers: `ssh g@ov8374 "docker ps | grep happy"`
- App logs: `ssh g@ov8374 "docker logs happy_app_1"`

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
