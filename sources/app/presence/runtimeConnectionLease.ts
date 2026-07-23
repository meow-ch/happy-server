import { db } from "@/storage/db";
import { Prisma } from "@prisma/client";
import { randomUUID } from "node:crypto";

/**
 * The CLI heartbeat is bounded to at most 60 seconds. Four minutes tolerates a
 * dropped maximum-cadence heartbeat plus scheduling/network jitter while still
 * bounding a crashed runtime's stale-positive readiness window.
 */
export const RUNTIME_CONNECTION_LEASE_DURATION_MS = 4 * 60_000;
export const RUNTIME_CONNECTION_PROTOCOL_VERSION = 1 as const;

/**
 * After a drained upgrade, rows last touched by the retired binary do not have
 * lease fields. Treat only their recent, active incarnation as connected, for
 * one bounded lease interval. Mixed-version writers are explicitly unsupported.
 * Once any lease marker has been written, the row is managed by the new
 * protocol and can never fall back to the legacy heuristic.
 */
export const LEGACY_RUNTIME_CONNECTION_GRACE_MS = RUNTIME_CONNECTION_LEASE_DURATION_MS;

// Owner-locked RPC delivery may wait up to 30 seconds for the selected
// runtime's acknowledgement. Prisma's default interactive-transaction timeout
// is shorter, which would release the advisory lock while the side effect was
// still in flight. The explicit bound preserves linearization. Because each
// such call occupies one pool connection, callers must keep the operation
// bounded and deployments must size the pool for concurrent RPC traffic.
export const RUNTIME_CONNECTION_OWNER_LOCK_MAX_WAIT_MS = 5_000;
export const RUNTIME_CONNECTION_OWNER_LOCK_TIMEOUT_MS = 40_000;

export type RuntimeConnectionOwnerOperationResult =
    | "completed"
    | "busy"
    | "not_owner";

export interface RuntimeConnectionSnapshot {
    active: boolean;
    activeInstanceId: string | null;
    lastActiveAt: Date;
    runtimeConnectionLeaseId?: string | null;
    runtimeConnectionLeaseInstanceId?: string | null;
    runtimeConnectionLeaseExpiresAt?: Date | null;
    runtimeInstanceRetired?: boolean;
}

export function isRuntimeConnected(
    session: RuntimeConnectionSnapshot,
    now = new Date(),
): boolean {
    if (!session.active
        || session.activeInstanceId === null
        || session.runtimeInstanceRetired === true) return false;

    const leaseManaged = session.runtimeConnectionLeaseId != null
        || session.runtimeConnectionLeaseInstanceId != null
        || session.runtimeConnectionLeaseExpiresAt != null;
    if (leaseManaged) {
        return session.runtimeConnectionLeaseId != null
            && session.runtimeConnectionLeaseInstanceId === session.activeInstanceId
            && session.runtimeConnectionLeaseExpiresAt != null
            && session.runtimeConnectionLeaseExpiresAt.getTime() > now.getTime();
    }

    return session.lastActiveAt.getTime()
        > now.getTime() - LEGACY_RUNTIME_CONNECTION_GRACE_MS;
}

interface ClaimRuntimeConnectionLeaseInput {
    accountId: string;
    sessionId: string;
    sessionInstanceId?: string;
    replayOnly: boolean;
    leaseId?: string;
}

export interface ClaimRuntimeConnectionLeaseResult {
    claimed: boolean;
    /** Present only when this socket owns the persisted lease generation. */
    ownedLeaseId?: string;
    /** Includes a server-inferred/synthetic incarnation for no-ID replay. */
    ownedSessionInstanceId?: string;
    /** Previous generation to evict after the replacement commits. */
    displacedLeaseId?: string;
}

/**
 * Claims a process incarnation and a socket-generation lease in one CAS.
 * Replay-only sockets are terminal outbox transports. Their first handshake
 * permanently retires the incarnation for live use, while a bounded exact
 * replay lease permits reconnecting until session-end or a successor wins.
 */
export async function claimRuntimeConnectionLease({
    accountId,
    sessionId,
    sessionInstanceId,
    replayOnly,
    leaseId = randomUUID(),
}: ClaimRuntimeConnectionLeaseInput): Promise<ClaimRuntimeConnectionLeaseResult> {
    // Database time is the authority for every lease transition. App-server
    // clock skew can therefore make neither a false-positive long lease nor an
    // immediately-expired one.
    if (replayOnly) {
        const suppliedInstanceId = sessionInstanceId ?? null;
        const syntheticInstanceId = randomUUID();
        const replayClaim = await db.$queryRaw<Array<{
            claimedCount: number;
            ownedSessionInstanceId: string | null;
            displacedLeaseId: string | null;
        }>>(Prisma.sql`
            WITH runtime_lock AS MATERIALIZED (
                SELECT pg_advisory_xact_lock(hashtextextended(${sessionId}, 0))
            ), candidate AS MATERIALIZED (
                SELECT session."id",
                       session."activeInstanceId" AS "previousInstanceId",
                       session."runtimeConnectionLeaseId" AS "previousLeaseId",
                       resolved."replayInstanceId"
                FROM "Session" session
                CROSS JOIN runtime_lock
                CROSS JOIN LATERAL (
                    SELECT CASE
                        WHEN ${suppliedInstanceId}::text IS NOT NULL
                            THEN ${suppliedInstanceId}::text
                        WHEN session."activeInstanceId" IS NULL
                            THEN ${syntheticInstanceId}
                        ELSE session."activeInstanceId"
                    END AS "replayInstanceId"
                ) resolved
                WHERE session."id" = ${sessionId}
                  AND session."accountId" = ${accountId}
                  AND (
                        (
                            ${suppliedInstanceId}::text IS NOT NULL
                            AND (
                                   session."activeInstanceId" IS NULL
                                OR session."activeInstanceId" = ${suppliedInstanceId}::text
                            )
                        )
                        OR (
                            ${suppliedInstanceId}::text IS NULL
                        )
                  )
                  AND (
                        ${suppliedInstanceId}::text IS NOT NULL
                        OR NOT session."active"
                        OR (
                               session."runtimeConnectionLeaseId" IS NOT NULL
                            OR session."runtimeConnectionLeaseInstanceId" IS NOT NULL
                            OR session."runtimeConnectionLeaseExpiresAt" IS NOT NULL
                        ) AND (
                               session."runtimeConnectionLeaseId" IS NULL
                            OR session."runtimeConnectionLeaseInstanceId"
                                IS DISTINCT FROM session."activeInstanceId"
                            OR session."runtimeConnectionLeaseExpiresAt" <= CURRENT_TIMESTAMP
                        )
                        OR (
                               session."runtimeConnectionLeaseId" IS NULL
                           AND session."runtimeConnectionLeaseInstanceId" IS NULL
                           AND session."runtimeConnectionLeaseExpiresAt" IS NULL
                           AND session."lastActiveAt" <= CURRENT_TIMESTAMP
                               - (${LEGACY_RUNTIME_CONNECTION_GRACE_MS} * INTERVAL '1 millisecond')
                        )
                  )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM "SessionRuntimeInstanceRetirement" retirement
                        WHERE retirement."sessionId" = session."id"
                          AND retirement."instanceId" = resolved."replayInstanceId"
                          AND retirement."status" <> 'replaying'
                  )
                FOR UPDATE OF session
            ), retired AS (
                INSERT INTO "SessionRuntimeInstanceRetirement" (
                    "sessionId", "instanceId", "status", "retiredAt"
                )
                SELECT candidate."id", candidate."replayInstanceId", 'replaying', CURRENT_TIMESTAMP
                FROM candidate
                ON CONFLICT ("sessionId", "instanceId") DO UPDATE
                SET "retiredAt" = CURRENT_TIMESTAMP
                WHERE "SessionRuntimeInstanceRetirement"."status" = 'replaying'
                RETURNING "sessionId", "instanceId"
            ), claimed AS (
                UPDATE "Session" session
                SET "active" = FALSE,
                    "activeInstanceId" = candidate."replayInstanceId",
                    "runtimeConnectionLeaseId" = ${leaseId},
                    "runtimeConnectionLeaseInstanceId" = candidate."replayInstanceId",
                    "runtimeConnectionLeaseExpiresAt" = CURRENT_TIMESTAMP
                        + (${RUNTIME_CONNECTION_LEASE_DURATION_MS} * INTERVAL '1 millisecond'),
                    "updatedAt" = CURRENT_TIMESTAMP
                FROM candidate
                INNER JOIN retired
                    ON retired."sessionId" = candidate."id"
                   AND retired."instanceId" = candidate."replayInstanceId"
                WHERE session."id" = candidate."id"
                RETURNING candidate."replayInstanceId",
                          candidate."previousLeaseId"
            )
            SELECT (SELECT COUNT(*)::integer FROM claimed) AS "claimedCount",
                   (SELECT "replayInstanceId" FROM claimed LIMIT 1) AS "ownedSessionInstanceId",
                   (SELECT "previousLeaseId" FROM claimed LIMIT 1) AS "displacedLeaseId"
        `);
        const row = replayClaim[0];
        return (row?.claimedCount ?? 0) > 0 && row.ownedSessionInstanceId
            ? {
                claimed: true,
                ownedLeaseId: leaseId,
                ownedSessionInstanceId: row.ownedSessionInstanceId,
                ...(row.displacedLeaseId
                    ? { displacedLeaseId: row.displacedLeaseId }
                    : {}),
            }
            : { claimed: false };
    }

    if (!sessionInstanceId) return { claimed: false };

    // The transaction-scoped advisory lock serializes every claim/session-end
    // transition for this session, including the no-current-row replay case.
    // The row lock additionally protects the candidate snapshot.
    // Retiring the displaced ID and installing its successor are one statement,
    // so no observer can see a successor without the predecessor blacklist.
    const claim = await db.$queryRaw<Array<{
        claimedCount: number;
        displacedLeaseId: string | null;
    }>>(Prisma.sql`
        WITH runtime_lock AS MATERIALIZED (
            SELECT pg_advisory_xact_lock(hashtextextended(${sessionId}, 0))
        ), candidate AS MATERIALIZED (
            SELECT session."id",
                   session."activeInstanceId" AS "previousInstanceId",
                   session."runtimeConnectionLeaseId" AS "previousLeaseId"
            FROM "Session" session
            CROSS JOIN runtime_lock
            WHERE session."id" = ${sessionId}
              AND session."accountId" = ${accountId}
              AND NOT EXISTS (
                    SELECT 1
                    FROM "SessionRuntimeInstanceRetirement" retirement
                    WHERE retirement."sessionId" = session."id"
                      AND retirement."instanceId" = ${sessionInstanceId}
              )
              AND (
                    session."activeInstanceId" IS NULL
                 OR session."activeInstanceId" = ${sessionInstanceId}
                 OR NOT session."active"
                 OR session."runtimeConnectionLeaseExpiresAt" <= CURRENT_TIMESTAMP
                 OR (
                        session."runtimeConnectionLeaseId" IS NULL
                    AND session."runtimeConnectionLeaseInstanceId" IS NULL
                    AND session."runtimeConnectionLeaseExpiresAt" IS NULL
                    AND session."lastActiveAt" <= CURRENT_TIMESTAMP
                        - (${LEGACY_RUNTIME_CONNECTION_GRACE_MS} * INTERVAL '1 millisecond')
                 )
              )
            FOR UPDATE
        ), retired AS (
            INSERT INTO "SessionRuntimeInstanceRetirement" (
                "sessionId", "instanceId", "status", "retiredAt"
            )
            SELECT candidate."id", candidate."previousInstanceId", 'superseded', CURRENT_TIMESTAMP
            FROM candidate
            WHERE candidate."previousInstanceId" IS NOT NULL
              AND candidate."previousInstanceId" <> ${sessionInstanceId}
            ON CONFLICT ("sessionId", "instanceId") DO UPDATE
            SET "status" = CASE
                    WHEN "SessionRuntimeInstanceRetirement"."status" = 'replaying'
                        THEN 'superseded'
                    ELSE "SessionRuntimeInstanceRetirement"."status"
                END,
                "retiredAt" = CURRENT_TIMESTAMP
            RETURNING 1
        ), claimed AS (
            UPDATE "Session" session
            SET "activeInstanceId" = ${sessionInstanceId},
                "runtimeConnectionLeaseId" = ${leaseId},
                "runtimeConnectionLeaseInstanceId" = ${sessionInstanceId},
                "runtimeConnectionLeaseExpiresAt" = CURRENT_TIMESTAMP
                    + (${RUNTIME_CONNECTION_LEASE_DURATION_MS} * INTERVAL '1 millisecond'),
                "active" = TRUE,
                "lastActiveAt" = CURRENT_TIMESTAMP,
                "updatedAt" = CURRENT_TIMESTAMP
            FROM candidate
            WHERE session."id" = candidate."id"
            RETURNING candidate."previousLeaseId"
        )
        SELECT (SELECT COUNT(*)::integer FROM claimed) AS "claimedCount",
               (SELECT COUNT(*)::integer FROM retired) AS "retiredCount",
               (SELECT "previousLeaseId" FROM claimed LIMIT 1) AS "displacedLeaseId"
    `);

    return (claim[0]?.claimedCount ?? 0) > 0
        ? {
            claimed: true,
            ownedLeaseId: leaseId,
            ownedSessionInstanceId: sessionInstanceId,
            ...(claim[0].displacedLeaseId
                ? { displacedLeaseId: claim[0].displacedLeaseId }
                : {}),
        }
        : { claimed: false };
}

interface ExpireRuntimeConnectionLeaseInput {
    accountId: string;
    sessionId: string;
    sessionInstanceId: string;
    leaseId: string;
    tombstoneLeaseId?: string;
}

/**
 * Expires only the exact process/socket generation that disconnected. Rotating
 * the lease ID is important: a heartbeat already queued by that socket can no
 * longer extend the tombstone after this write commits.
 */
export async function expireRuntimeConnectionLease({
    accountId,
    sessionId,
    sessionInstanceId,
    leaseId,
    tombstoneLeaseId = randomUUID(),
}: ExpireRuntimeConnectionLeaseInput): Promise<boolean> {
    const expired = await db.$executeRaw(Prisma.sql`
        WITH runtime_lock AS MATERIALIZED (
            SELECT pg_advisory_xact_lock(hashtextextended(${sessionId}, 0))
        )
        UPDATE "Session" session
        SET "runtimeConnectionLeaseId" = ${tombstoneLeaseId},
            "runtimeConnectionLeaseInstanceId" = ${sessionInstanceId},
            "runtimeConnectionLeaseExpiresAt" = CURRENT_TIMESTAMP,
            "updatedAt" = CURRENT_TIMESTAMP
        FROM runtime_lock
        WHERE session."id" = ${sessionId}
          AND session."accountId" = ${accountId}
          AND session."active" = TRUE
          AND session."activeInstanceId" = ${sessionInstanceId}
          AND session."runtimeConnectionLeaseInstanceId" = ${sessionInstanceId}
          AND session."runtimeConnectionLeaseId" = ${leaseId}
          AND NOT EXISTS (
                SELECT 1
                FROM "SessionRuntimeInstanceRetirement" retirement
                WHERE retirement."sessionId" = ${sessionId}
                  AND retirement."instanceId" = ${sessionInstanceId}
          )
    `);
    return expired > 0;
}

interface RenewRuntimeConnectionLeaseInput {
    accountId: string;
    sessionId: string;
    sessionInstanceId: string;
    leaseId: string;
}

/** Renews only the exact active process/socket generation, using DB time. */
export async function renewRuntimeConnectionLease({
    accountId,
    sessionId,
    sessionInstanceId,
    leaseId,
}: RenewRuntimeConnectionLeaseInput): Promise<boolean> {
    const renewed = await db.$executeRaw(Prisma.sql`
        UPDATE "Session"
        SET "runtimeConnectionLeaseExpiresAt" = CURRENT_TIMESTAMP
                + (${RUNTIME_CONNECTION_LEASE_DURATION_MS} * INTERVAL '1 millisecond'),
            "lastActiveAt" = CURRENT_TIMESTAMP,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = ${sessionId}
          AND "accountId" = ${accountId}
          AND "active" = TRUE
          AND "activeInstanceId" = ${sessionInstanceId}
          AND "runtimeConnectionLeaseInstanceId" = ${sessionInstanceId}
          AND "runtimeConnectionLeaseId" = ${leaseId}
          AND NOT EXISTS (
                SELECT 1
                FROM "SessionRuntimeInstanceRetirement" retirement
                WHERE retirement."sessionId" = ${sessionId}
                  AND retirement."instanceId" = ${sessionInstanceId}
          )
    `);
    return renewed > 0;
}

interface RenewLegacyRuntimeConnectionInput {
    accountId: string;
    sessionId: string;
}

interface CheckRuntimeConnectionOwnershipInput {
    accountId: string;
    sessionId: string;
    sessionInstanceId?: string;
    leaseId?: string;
}

interface RuntimeConnectionWriteInput extends CheckRuntimeConnectionOwnershipInput {
    expectedVersion: number;
}

interface RuntimeConnectionMetadataWriteInput extends RuntimeConnectionWriteInput {
    metadata: string;
}

interface RuntimeConnectionStateWriteInput extends RuntimeConnectionWriteInput {
    agentState: string | null;
}

function runtimeConnectionOwnershipSql(
    sessionInstanceId: string | undefined,
    leaseId: string | undefined,
): Prisma.Sql {
    if ((sessionInstanceId === undefined) !== (leaseId === undefined)) {
        return Prisma.sql`FALSE`;
    }
    if (!sessionInstanceId) {
        return Prisma.sql`
               session."active" = TRUE
           AND session."activeInstanceId" IS NULL
           AND session."runtimeConnectionLeaseId" IS NULL
           AND session."runtimeConnectionLeaseInstanceId" IS NULL
           AND session."runtimeConnectionLeaseExpiresAt" IS NULL
        `;
    }
    return Prisma.sql`
           session."active" = TRUE
       AND session."activeInstanceId" = ${sessionInstanceId}
       AND session."runtimeConnectionLeaseInstanceId" = ${sessionInstanceId}
       AND session."runtimeConnectionLeaseId" = ${leaseId}
       AND session."runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP
       AND NOT EXISTS (
            SELECT 1
            FROM "SessionRuntimeInstanceRetirement" retirement
            WHERE retirement."sessionId" = session."id"
              AND retirement."instanceId" = ${sessionInstanceId}
       )
    `;
}

/**
 * Authorizes a socket packet against the exact persisted generation. Supplying
 * neither incarnation nor lease selects the tightly-scoped legacy path;
 * partial tuples always fail closed.
 */
export async function isRuntimeConnectionOwner({
    accountId,
    sessionId,
    sessionInstanceId,
    leaseId,
}: CheckRuntimeConnectionOwnershipInput): Promise<boolean> {
    if ((sessionInstanceId === undefined) !== (leaseId === undefined)) return false;

    const rows = await db.$queryRaw<Array<{ owned: boolean }>>(sessionInstanceId
        ? Prisma.sql`
            SELECT EXISTS (
                SELECT 1
                FROM "Session" session
                WHERE session."id" = ${sessionId}
                  AND session."accountId" = ${accountId}
                  AND session."active" = TRUE
                  AND session."activeInstanceId" = ${sessionInstanceId}
                  AND session."runtimeConnectionLeaseInstanceId" = ${sessionInstanceId}
                  AND session."runtimeConnectionLeaseId" = ${leaseId}
                  AND session."runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP
                  AND NOT EXISTS (
                        SELECT 1
                        FROM "SessionRuntimeInstanceRetirement" retirement
                        WHERE retirement."sessionId" = session."id"
                          AND retirement."instanceId" = ${sessionInstanceId}
                  )
            ) AS "owned"
        `
        : Prisma.sql`
            SELECT EXISTS (
                SELECT 1
                FROM "Session" session
                WHERE session."id" = ${sessionId}
                  AND session."accountId" = ${accountId}
                  AND session."active" = TRUE
                  AND session."activeInstanceId" IS NULL
                  AND session."runtimeConnectionLeaseId" IS NULL
                  AND session."runtimeConnectionLeaseInstanceId" IS NULL
                  AND session."runtimeConnectionLeaseExpiresAt" IS NULL
            ) AS "owned"
        `);
    return rows[0]?.owned === true;
}

/**
 * Linearizes a bounded external side effect (currently RPC
 * registration/delivery) with claim/end. The advisory lock is fail-fast: a
 * caller never spends its transaction timeout queued behind another side
 * effect and therefore never starts an operation it cannot fence to completion.
 * `busy` is distinct from stale ownership and is safe to retry.
 */
export async function runWithRuntimeConnectionOwnerLock(
    {
        accountId,
        sessionId,
        sessionInstanceId,
        leaseId,
    }: CheckRuntimeConnectionOwnershipInput,
    operation: (tx: Prisma.TransactionClient) => Promise<void>,
): Promise<RuntimeConnectionOwnerOperationResult> {
    return db.$transaction(async (tx) => {
        const rows = await tx.$queryRaw<Array<{
            lockAcquired: boolean;
            owned: boolean;
        }>>(Prisma.sql`
            WITH runtime_lock AS MATERIALIZED (
                SELECT pg_try_advisory_xact_lock(
                    hashtextextended(${sessionId}, 0)
                ) AS "lockAcquired"
            )
            SELECT runtime_lock."lockAcquired",
                   CASE WHEN runtime_lock."lockAcquired" THEN EXISTS (
                       SELECT 1
                       FROM "Session" session
                       WHERE session."id" = ${sessionId}
                         AND session."accountId" = ${accountId}
                         AND (${runtimeConnectionOwnershipSql(sessionInstanceId, leaseId)})
                   ) ELSE FALSE END AS "owned"
            FROM runtime_lock
        `);
        if (rows[0]?.lockAcquired !== true) return "busy";
        if (rows[0]?.owned !== true) return "not_owner";
        await operation(tx);
        return "completed";
    }, {
        maxWait: RUNTIME_CONNECTION_OWNER_LOCK_MAX_WAIT_MS,
        timeout: RUNTIME_CONNECTION_OWNER_LOCK_TIMEOUT_MS,
    });
}

/** Atomic metadata CAS with the same DB-clock lease predicate as readiness. */
export async function updateRuntimeSessionMetadata({
    accountId,
    sessionId,
    sessionInstanceId,
    leaseId,
    expectedVersion,
    metadata,
}: RuntimeConnectionMetadataWriteInput): Promise<boolean> {
    const updated = await db.$executeRaw(Prisma.sql`
        UPDATE "Session" session
        SET "metadata" = ${metadata},
            "metadataVersion" = ${expectedVersion + 1},
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE session."id" = ${sessionId}
          AND session."accountId" = ${accountId}
          AND session."metadataVersion" = ${expectedVersion}
          AND (${runtimeConnectionOwnershipSql(sessionInstanceId, leaseId)})
    `);
    return updated > 0;
}

/** Atomic agent-state CAS with the same DB-clock lease predicate as readiness. */
export async function updateRuntimeSessionState({
    accountId,
    sessionId,
    sessionInstanceId,
    leaseId,
    expectedVersion,
    agentState,
}: RuntimeConnectionStateWriteInput): Promise<boolean> {
    const updated = await db.$executeRaw(Prisma.sql`
        UPDATE "Session" session
        SET "agentState" = ${agentState},
            "agentStateVersion" = ${expectedVersion + 1},
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE session."id" = ${sessionId}
          AND session."accountId" = ${accountId}
          AND session."agentStateVersion" = ${expectedVersion}
          AND (${runtimeConnectionOwnershipSql(sessionInstanceId, leaseId)})
    `);
    return updated > 0;
}

/**
 * Records activity from a pre-incarnation client only while the row is wholly
 * legacy. Requiring a null active incarnation prevents an old client from
 * refreshing the bounded fallback for a newer incarnation it does not own.
 */
export async function renewLegacyRuntimeConnection({
    accountId,
    sessionId,
}: RenewLegacyRuntimeConnectionInput): Promise<boolean> {
    const renewed = await db.$executeRaw(Prisma.sql`
        UPDATE "Session"
        SET "active" = TRUE,
            "lastActiveAt" = CURRENT_TIMESTAMP,
            "updatedAt" = CURRENT_TIMESTAMP
        WHERE "id" = ${sessionId}
          AND "accountId" = ${accountId}
          AND "activeInstanceId" IS NULL
          AND "runtimeConnectionLeaseId" IS NULL
          AND "runtimeConnectionLeaseInstanceId" IS NULL
          AND "runtimeConnectionLeaseExpiresAt" IS NULL
    `);
    return renewed > 0;
}

interface EndRuntimeConnectionLeaseInput {
    accountId: string;
    sessionId: string;
    sessionInstanceId: string;
    tombstoneLeaseId?: string;
}

/**
 * A durable session-end belongs to the process incarnation, not a transient
 * socket generation: an offline outbox replay must still be able to end the
 * process after its socket lease is gone. A resumed/new process must therefore
 * use a fresh sessionInstanceId, as ApiSession does.
 */
export async function endRuntimeConnectionLease({
    accountId,
    sessionId,
    sessionInstanceId,
    tombstoneLeaseId = randomUUID(),
}: EndRuntimeConnectionLeaseInput): Promise<Date | null> {
    const ended = await db.$queryRaw<Array<{ lastActiveAt: Date }>>(Prisma.sql`
        WITH runtime_lock AS MATERIALIZED (
            SELECT pg_advisory_xact_lock(hashtextextended(${sessionId}, 0))
        ), ended AS (
            UPDATE "Session" session
            SET "active" = FALSE,
                "lastActiveAt" = CURRENT_TIMESTAMP,
                "runtimeConnectionLeaseId" = ${tombstoneLeaseId},
                "runtimeConnectionLeaseInstanceId" = ${sessionInstanceId},
                "runtimeConnectionLeaseExpiresAt" = CURRENT_TIMESTAMP,
                "updatedAt" = CURRENT_TIMESTAMP
            FROM runtime_lock
            WHERE session."id" = ${sessionId}
              AND session."accountId" = ${accountId}
              AND session."activeInstanceId" = ${sessionInstanceId}
              AND NOT EXISTS (
                    SELECT 1
                    FROM "SessionRuntimeInstanceRetirement" retirement
                    WHERE retirement."sessionId" = session."id"
                      AND retirement."instanceId" = ${sessionInstanceId}
                      AND retirement."status" <> 'replaying'
              )
            RETURNING session."id", session."lastActiveAt"
        ), retired AS (
            INSERT INTO "SessionRuntimeInstanceRetirement" (
                "sessionId", "instanceId", "status", "retiredAt"
            )
            SELECT ended."id", ${sessionInstanceId}, 'ended', CURRENT_TIMESTAMP
            FROM ended
            ON CONFLICT ("sessionId", "instanceId") DO UPDATE
            SET "status" = 'ended',
                "retiredAt" = CURRENT_TIMESTAMP
            RETURNING 1
        )
        SELECT ended."lastActiveAt",
               (SELECT COUNT(*)::integer FROM retired) AS "retiredCount"
        FROM ended
    `);
    return ended[0]?.lastActiveAt ?? null;
}

export interface RuntimeConnectionSessionRead {
    id: string;
    agentStateVersion: number;
    runtimeConnectionProtocolVersion: 1;
    runtimeConnected: boolean;
    runtimeInstanceId: string | null;
    runtimeLeaseInstanceId: string | null;
    runtimeLeaseExpiresAt: Date | null;
    runtimeInstanceRetired: boolean;
    runtimeConnectionCheckedAt: Date;
    dataEncryptionKey: Uint8Array | null;
}

/** Reads the session and evaluates its lease against the same database clock. */
export async function findRuntimeConnectionSession(
    accountId: string,
    sessionId: string,
): Promise<RuntimeConnectionSessionRead | null> {
    const sessions = await db.$queryRaw<RuntimeConnectionSessionRead[]>(Prisma.sql`
        SELECT "id",
               "agentStateVersion",
               1 AS "runtimeConnectionProtocolVersion",
               "dataEncryptionKey",
               "activeInstanceId" AS "runtimeInstanceId",
               "runtimeConnectionLeaseInstanceId" AS "runtimeLeaseInstanceId",
               "runtimeConnectionLeaseExpiresAt" AS "runtimeLeaseExpiresAt",
               EXISTS (
                   SELECT 1
                   FROM "SessionRuntimeInstanceRetirement" retirement
                   WHERE retirement."sessionId" = "Session"."id"
                     AND retirement."instanceId" = "Session"."activeInstanceId"
               ) AS "runtimeInstanceRetired",
               CURRENT_TIMESTAMP AS "runtimeConnectionCheckedAt",
               CASE
                   WHEN NOT "active"
                     OR "activeInstanceId" IS NULL
                     OR EXISTS (
                         SELECT 1
                         FROM "SessionRuntimeInstanceRetirement" retirement
                         WHERE retirement."sessionId" = "Session"."id"
                           AND retirement."instanceId" = "Session"."activeInstanceId"
                     )
                   THEN FALSE
                   WHEN "runtimeConnectionLeaseId" IS NOT NULL
                     OR "runtimeConnectionLeaseInstanceId" IS NOT NULL
                     OR "runtimeConnectionLeaseExpiresAt" IS NOT NULL
                   THEN "runtimeConnectionLeaseId" IS NOT NULL
                    AND "runtimeConnectionLeaseInstanceId" = "activeInstanceId"
                    AND "runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP
                   ELSE "lastActiveAt" > CURRENT_TIMESTAMP
                    - (${LEGACY_RUNTIME_CONNECTION_GRACE_MS} * INTERVAL '1 millisecond')
               END AS "runtimeConnected"
        FROM "Session"
        WHERE "id" = ${sessionId}
          AND "accountId" = ${accountId}
        LIMIT 1
    `);
    return sessions[0] ?? null;
}

/** A fresh tombstone also fences already-queued heartbeats after session-end. */
export function newRuntimeConnectionLeaseTombstone(): string {
    return randomUUID();
}
