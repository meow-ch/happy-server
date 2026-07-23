import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    executeRaw: vi.fn(),
    queryRaw: vi.fn(),
    transaction: vi.fn(),
    transactionQueryRaw: vi.fn(),
}));

vi.mock("@/storage/db", () => ({
    db: {
        $executeRaw: mocks.executeRaw,
        $queryRaw: mocks.queryRaw,
        $transaction: mocks.transaction,
    },
}));

import {
    claimRuntimeConnectionLease,
    endRuntimeConnectionLease,
    expireRuntimeConnectionLease,
    findRuntimeConnectionSession,
    isRuntimeConnected,
    isRuntimeConnectionOwner,
    LEGACY_RUNTIME_CONNECTION_GRACE_MS,
    renewLegacyRuntimeConnection,
    renewRuntimeConnectionLease,
    RUNTIME_CONNECTION_LEASE_DURATION_MS,
    RUNTIME_CONNECTION_OWNER_LOCK_MAX_WAIT_MS,
    RUNTIME_CONNECTION_OWNER_LOCK_TIMEOUT_MS,
    runWithRuntimeConnectionOwnerLock,
    updateRuntimeSessionMetadata,
    updateRuntimeSessionState,
} from "@/app/presence/runtimeConnectionLease";

const instanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
const newerInstanceId = "8c46b5ad-4155-47ed-a470-d21c7be49baf";
const now = new Date("2026-07-22T18:00:00.000Z");

function leaseSnapshot(overrides: Record<string, unknown> = {}) {
    return {
        active: true,
        activeInstanceId: instanceId,
        lastActiveAt: now,
        runtimeConnectionLeaseId: "lease-generation-1",
        runtimeConnectionLeaseInstanceId: instanceId,
        runtimeConnectionLeaseExpiresAt: new Date(now.getTime() + RUNTIME_CONNECTION_LEASE_DURATION_MS),
        runtimeInstanceRetired: false,
        ...overrides,
    };
}

function renderedSql(call: unknown[]): string {
    const query = call[0] as { strings?: readonly string[] };
    return query.strings?.join("?").replace(/\s+/g, " ").trim() ?? "";
}

describe("runtime connection lease", () => {
    beforeEach(() => {
        mocks.executeRaw.mockReset();
        mocks.queryRaw.mockReset();
        mocks.transaction.mockReset();
        mocks.transactionQueryRaw.mockReset();
    });

    it("fails closed at expiry, for retired incarnations, and for partial tuples", () => {
        expect(isRuntimeConnected(leaseSnapshot(), now)).toBe(true);
        expect(isRuntimeConnected(leaseSnapshot({
            runtimeConnectionLeaseExpiresAt: now,
        }), now)).toBe(false);
        expect(isRuntimeConnected(leaseSnapshot({ runtimeInstanceRetired: true }), now)).toBe(false);
        expect(isRuntimeConnected(leaseSnapshot({ runtimeConnectionLeaseId: null }), now)).toBe(false);
        expect(isRuntimeConnected(leaseSnapshot({
            runtimeConnectionLeaseInstanceId: newerInstanceId,
        }), now)).toBe(false);
    });

    it("tolerates a dropped maximum-cadence heartbeat but expires at the exact lease boundary", () => {
        expect(RUNTIME_CONNECTION_LEASE_DURATION_MS).toBeGreaterThanOrEqual(3 * 60_000);
        expect(isRuntimeConnected(leaseSnapshot(), new Date(now.getTime() + 2 * 60_000 + 1)))
            .toBe(true);
        expect(isRuntimeConnected(leaseSnapshot(), new Date(now.getTime() + RUNTIME_CONNECTION_LEASE_DURATION_MS)))
            .toBe(false);
    });

    it("uses only a bounded fallback for untouched legacy rows", () => {
        const legacy = {
            active: true,
            activeInstanceId: instanceId,
            lastActiveAt: new Date(now.getTime() - LEGACY_RUNTIME_CONNECTION_GRACE_MS + 1),
            runtimeConnectionLeaseId: null,
            runtimeConnectionLeaseInstanceId: null,
            runtimeConnectionLeaseExpiresAt: null,
            runtimeInstanceRetired: false,
        };
        expect(isRuntimeConnected(legacy, now)).toBe(true);
        expect(isRuntimeConnected({
            ...legacy,
            lastActiveAt: new Date(now.getTime() - LEGACY_RUNTIME_CONNECTION_GRACE_MS),
        }, now)).toBe(false);
    });

    it("serializes a live claim, retires its predecessor, and returns the displaced generation", async () => {
        mocks.queryRaw.mockResolvedValue([{
            claimedCount: 1,
            displacedLeaseId: "old-lease",
        }]);

        await expect(claimRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            replayOnly: false,
            leaseId: "lease-generation-1",
        })).resolves.toEqual({
            claimed: true,
            ownedLeaseId: "lease-generation-1",
            ownedSessionInstanceId: instanceId,
            displacedLeaseId: "old-lease",
        });

        const sql = renderedSql(mocks.queryRaw.mock.calls[0]);
        expect(sql).toContain("pg_advisory_xact_lock");
        expect(sql).toContain("FOR UPDATE");
        expect(sql).toContain("'superseded'");
        expect(sql).toContain('"runtimeConnectionLeaseExpiresAt" <= CURRENT_TIMESTAMP');
        expect(sql).toContain('"SessionRuntimeInstanceRetirement"');
        expect((mocks.queryRaw.mock.calls[0][0] as { values: unknown[] }).values)
            .toContain(RUNTIME_CONNECTION_LEASE_DURATION_MS);
    });

    it("fails a normal claim when the incarnation is retired or another live incarnation owns it", async () => {
        mocks.queryRaw.mockResolvedValue([{ claimedCount: 0, displacedLeaseId: null }]);
        await expect(claimRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            replayOnly: false,
            leaseId: "stale-lease",
        })).resolves.toEqual({ claimed: false });
    });

    it("turns replay handshake into a finite terminal lease and supports inferred incarnation IDs", async () => {
        mocks.queryRaw.mockResolvedValue([{
            claimedCount: 1,
            ownedSessionInstanceId: instanceId,
            displacedLeaseId: "live-lease",
        }]);

        await expect(claimRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            replayOnly: true,
            leaseId: "replay-lease",
        })).resolves.toEqual({
            claimed: true,
            ownedLeaseId: "replay-lease",
            ownedSessionInstanceId: instanceId,
            displacedLeaseId: "live-lease",
        });

        const sql = renderedSql(mocks.queryRaw.mock.calls[0]);
        expect(sql).toContain('"active" = FALSE');
        expect(sql).toContain("'replaying'");
        expect(sql).toContain('retirement."status" <> \'replaying\'');
        expect(sql).toContain("pg_advisory_xact_lock");
        expect(sql).toContain("CURRENT_TIMESTAMP +");
    });

    it("does not grant a replay lease when a different live generation is authoritative", async () => {
        mocks.queryRaw.mockResolvedValue([{
            claimedCount: 0,
            ownedSessionInstanceId: null,
            displacedLeaseId: null,
        }]);
        await expect(claimRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            replayOnly: true,
            leaseId: "replay-lease",
        })).resolves.toEqual({ claimed: false });
    });

    it("renews and expires only the exact non-retired socket generation", async () => {
        mocks.executeRaw.mockResolvedValueOnce(1).mockResolvedValueOnce(1);
        await expect(renewRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
        })).resolves.toBe(true);
        await expect(expireRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
            tombstoneLeaseId: "expired-tombstone",
        })).resolves.toBe(true);

        for (const call of mocks.executeRaw.mock.calls) {
            const sql = renderedSql(call);
            expect(sql).toContain('"runtimeConnectionLeaseId" =');
            expect(sql).toContain('"activeInstanceId" =');
            expect(sql).toContain('"SessionRuntimeInstanceRetirement"');
        }
        expect(renderedSql(mocks.executeRaw.mock.calls[1])).toContain("pg_advisory_xact_lock");
    });

    it("authorizes and mutates runtime state with PostgreSQL time, never the app clock", async () => {
        mocks.queryRaw.mockResolvedValue([{ owned: true }]);
        mocks.executeRaw.mockResolvedValue(1);

        await expect(isRuntimeConnectionOwner({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
        })).resolves.toBe(true);
        await expect(updateRuntimeSessionMetadata({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
            expectedVersion: 3,
            metadata: "encrypted-metadata",
        })).resolves.toBe(true);
        await expect(updateRuntimeSessionState({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
            expectedVersion: 5,
            agentState: "encrypted-state",
        })).resolves.toBe(true);

        expect(renderedSql(mocks.queryRaw.mock.calls[0])).toContain(
            '"runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP',
        );
        expect(renderedSql(mocks.executeRaw.mock.calls[0])).toContain(
            '"runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP',
        );
        expect(renderedSql(mocks.executeRaw.mock.calls[1])).toContain(
            '"runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP',
        );
        expect(JSON.stringify(mocks.executeRaw.mock.calls)).not.toContain(now.toISOString());
    });

    it("keeps the advisory owner lock alive beyond the bounded RPC acknowledgement", async () => {
        const tx = { $queryRaw: mocks.transactionQueryRaw };
        mocks.transactionQueryRaw.mockResolvedValue([{ lockAcquired: true, owned: true }]);
        mocks.transaction.mockImplementation(async (operation) => operation(tx));
        const operation = vi.fn(async (client) => {
            expect(client).toBe(tx);
        });

        await expect(runWithRuntimeConnectionOwnerLock({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
        }, operation)).resolves.toBe("completed");

        expect(operation).toHaveBeenCalledTimes(1);
        expect(mocks.transaction).toHaveBeenCalledWith(expect.any(Function), {
            maxWait: RUNTIME_CONNECTION_OWNER_LOCK_MAX_WAIT_MS,
            timeout: RUNTIME_CONNECTION_OWNER_LOCK_TIMEOUT_MS,
        });
        expect(RUNTIME_CONNECTION_OWNER_LOCK_TIMEOUT_MS).toBeGreaterThan(30_000);
        expect(renderedSql(mocks.transactionQueryRaw.mock.calls[0])).toContain("pg_try_advisory_xact_lock");
    });

    it("fails fast without starting the operation when the session owner lock is busy", async () => {
        const tx = { $queryRaw: mocks.transactionQueryRaw };
        mocks.transactionQueryRaw.mockResolvedValue([{ lockAcquired: false, owned: false }]);
        mocks.transaction.mockImplementation(async (operation) => operation(tx));
        const operation = vi.fn();

        await expect(runWithRuntimeConnectionOwnerLock({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            leaseId: "lease-generation-1",
        }, operation)).resolves.toBe("busy");

        expect(operation).not.toHaveBeenCalled();
        const sql = renderedSql(mocks.transactionQueryRaw.mock.calls[0]);
        expect(sql).toContain("pg_try_advisory_xact_lock");
        expect(sql).not.toMatch(/SELECT pg_advisory_xact_lock\(/);
    });

    it("keeps legacy renewal fenced to an untouched, unincarnated row", async () => {
        mocks.executeRaw.mockResolvedValue(1);
        await expect(renewLegacyRuntimeConnection({
            accountId: "account-1",
            sessionId: "session-1",
        })).resolves.toBe(true);
        const sql = renderedSql(mocks.executeRaw.mock.calls[0]);
        expect(sql).toContain('"activeInstanceId" IS NULL');
        expect(sql).toContain('"runtimeConnectionLeaseId" IS NULL');
    });

    it("ends an inactive or replaying current incarnation and records durable ended status", async () => {
        const endedAt = new Date("2026-07-22T18:00:01.000Z");
        mocks.queryRaw.mockResolvedValue([{ lastActiveAt: endedAt }]);

        await expect(endRuntimeConnectionLease({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: instanceId,
            tombstoneLeaseId: "ended-tombstone",
        })).resolves.toEqual(endedAt);

        const sql = renderedSql(mocks.queryRaw.mock.calls[0]);
        expect(sql).toContain("pg_advisory_xact_lock");
        expect(sql).toContain('"active" = FALSE');
        expect(sql).not.toContain('session."active" = TRUE');
        expect(sql).toContain("'replaying'");
        expect(sql).toContain("'ended'");
    });

    it("reads readiness, retirement, and expiry from one DB-time snapshot", async () => {
        const row = {
            id: "session-1",
            agentStateVersion: 0,
            runtimeConnectionProtocolVersion: 1 as const,
            runtimeConnected: false,
            runtimeInstanceId: instanceId,
            runtimeLeaseInstanceId: instanceId,
            runtimeLeaseExpiresAt: now,
            runtimeInstanceRetired: true,
            runtimeConnectionCheckedAt: now,
            dataEncryptionKey: null,
        };
        mocks.queryRaw.mockResolvedValue([row]);
        await expect(findRuntimeConnectionSession("account-1", "session-1"))
            .resolves.toEqual(row);
        const sql = renderedSql(mocks.queryRaw.mock.calls[0]);
        expect(sql).toContain('"runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP');
        expect(sql).toContain('END AS "runtimeConnected"');
    });
});
