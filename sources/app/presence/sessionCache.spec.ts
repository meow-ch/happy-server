import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    sessionFindUnique: vi.fn(),
    sessionUpdateMany: vi.fn(),
    machineFindUnique: vi.fn(),
    machineUpdate: vi.fn(),
    log: vi.fn()
}));

vi.mock("@/storage/db", () => ({
    db: {
        session: {
            findUnique: mocks.sessionFindUnique,
            updateMany: mocks.sessionUpdateMany
        },
        machine: {
            findUnique: mocks.machineFindUnique,
            update: mocks.machineUpdate
        }
    }
}));
vi.mock("@/utils/log", () => ({ log: mocks.log }));
vi.mock("@/app/monitoring/metrics2", () => ({
    sessionCacheCounter: { inc: vi.fn() },
    databaseUpdatesSkippedCounter: { inc: vi.fn() },
    activityCacheFlushCounter: { inc: vi.fn() },
    activityCacheFlushDurationHistogram: { observe: vi.fn() },
    activityCachePendingUpdatesGauge: { set: vi.fn() },
    activityCacheDatabaseUpdatesCounter: { inc: vi.fn() },
    activityCacheFlushInProgressGauge: { set: vi.fn() }
}));

import { ActivityCache, activityCache } from "@/app/presence/sessionCache";

describe("ActivityCache session timestamp fencing", () => {
    let cache: ActivityCache;

    beforeEach(() => {
        mocks.sessionFindUnique.mockReset();
        mocks.sessionUpdateMany.mockReset();
        mocks.machineFindUnique.mockReset();
        mocks.machineUpdate.mockReset();
        cache = new ActivityCache({ autoStart: false });
    });

    afterEach(async () => {
        await cache.shutdown();
        await activityCache.shutdown();
    });

    it("flushes active=true only when the heartbeat is newer than persisted state", async () => {
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany.mockResolvedValue({ count: 0 });

        expect(await cache.isSessionValid("session-1", "account-1")).toBe(true);
        expect(cache.queueSessionUpdate(
            "session-1",
            40_000,
            "90b85ebd-6bb8-41bb-aa2d-681765e24f0d"
        )).toBe(true);
        await cache.flushPendingUpdates();

        expect(mocks.sessionUpdateMany).toHaveBeenCalledWith({
            where: {
                id: "session-1",
                lastActiveAt: { lt: new Date(40_000) },
                activeInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
                active: true,
            },
            data: { lastActiveAt: new Date(40_000), active: true }
        });
    });

    it("runs only one flush at a time and drains a newer update requested in flight", async () => {
        let resolveFirst!: (value: { count: number }) => void;
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany
            .mockImplementationOnce(() => new Promise(resolve => { resolveFirst = resolve; }))
            .mockResolvedValueOnce({ count: 1 });

        await cache.isSessionValid("session-1", "account-1");
        cache.queueSessionUpdate("session-1", 40_000);
        const firstFlush = cache.flushPendingUpdates();
        await vi.waitFor(() => expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1));

        cache.queueSessionUpdate("session-1", 80_000);
        const overlappingFlush = cache.flushPendingUpdates();
        expect(overlappingFlush).toBe(firstFlush);
        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1);

        resolveFirst({ count: 1 });
        await firstFlush;

        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(2);
        expect(mocks.sessionUpdateMany.mock.calls[1][0].data.lastActiveAt)
            .toEqual(new Date(80_000));
    });

    it("does not queue a redundant session write behind an in-flight watermark", async () => {
        let resolveFirst!: (value: { count: number }) => void;
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany.mockImplementationOnce(
            () => new Promise(resolve => { resolveFirst = resolve; })
        );

        await cache.isSessionValid("session-1", "account-1");
        cache.queueSessionUpdate("session-1", 40_000);
        const flush = cache.flushPendingUpdates();
        await vi.waitFor(() => expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1));

        expect(cache.queueSessionUpdate("session-1", 50_000)).toBe(false);
        expect(cache.flushPendingUpdates()).toBe(flush);
        resolveFirst({ count: 1 });
        await flush;

        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1);
    });

    it("does not suppress a new session incarnation behind an in-flight heartbeat", async () => {
        let resolveFirst!: (value: { count: number }) => void;
        const firstInstanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
        const secondInstanceId = "8c46b5ad-4155-47ed-a470-d21c7be49baf";
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany
            .mockImplementationOnce(() => new Promise(resolve => { resolveFirst = resolve; }))
            .mockResolvedValueOnce({ count: 1 });

        await cache.isSessionValid("session-1", "account-1");
        cache.queueSessionUpdate("session-1", 40_000, firstInstanceId);
        const firstFlush = cache.flushPendingUpdates();
        await vi.waitFor(() => expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1));

        expect(cache.queueSessionUpdate("session-1", 50_000, secondInstanceId)).toBe(true);
        expect(cache.flushPendingUpdates()).toBe(firstFlush);
        resolveFirst({ count: 1 });
        await firstFlush;

        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(2);
        expect(mocks.sessionUpdateMany.mock.calls[1][0].where.activeInstanceId)
            .toBe(secondInstanceId);
    });

    it("does not queue a redundant machine write behind an in-flight watermark", async () => {
        let resolveFirst!: (value: { id: string }) => void;
        mocks.machineFindUnique.mockResolvedValue({
            id: "machine-1",
            lastActiveAt: new Date(0)
        });
        mocks.machineUpdate.mockImplementationOnce(
            () => new Promise(resolve => { resolveFirst = resolve; })
        );

        await cache.isMachineValid("machine-1", "account-1");
        cache.queueMachineUpdate("machine-1", 40_000);
        const flush = cache.flushPendingUpdates();
        await vi.waitFor(() => expect(mocks.machineUpdate).toHaveBeenCalledTimes(1));

        expect(cache.queueMachineUpdate("machine-1", 50_000)).toBe(false);
        expect(cache.flushPendingUpdates()).toBe(flush);
        resolveFirst({ id: "machine-1" });
        await flush;

        expect(mocks.machineUpdate).toHaveBeenCalledTimes(1);
    });

    it("preserves a newer queued heartbeat when an older database update fails", async () => {
        let rejectFirst!: (error: Error) => void;
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany
            .mockImplementationOnce(() => new Promise((_resolve, reject) => { rejectFirst = reject; }))
            .mockResolvedValueOnce({ count: 1 });

        await cache.isSessionValid("session-1", "account-1");
        cache.queueSessionUpdate("session-1", 40_000);
        const failedFlush = cache.flushPendingUpdates();
        await vi.waitFor(() => expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1));
        cache.queueSessionUpdate("session-1", 80_000);
        rejectFirst(new Error("database unavailable"));
        await failedFlush;

        await cache.flushPendingUpdates();

        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(2);
        expect(mocks.sessionUpdateMany.mock.calls[1][0].data.lastActiveAt)
            .toEqual(new Date(80_000));
    });

    it("does not replace queued state when concurrent cache validations resolve out of order", async () => {
        let resolveFirst!: (value: { id: string; lastActiveAt: Date }) => void;
        let resolveSecond!: (value: { id: string; lastActiveAt: Date }) => void;
        mocks.sessionFindUnique
            .mockImplementationOnce(() => new Promise(resolve => { resolveFirst = resolve; }))
            .mockImplementationOnce(() => new Promise(resolve => { resolveSecond = resolve; }));
        mocks.sessionUpdateMany.mockResolvedValue({ count: 1 });

        const firstValidation = cache.isSessionValid("session-1", "account-1");
        const secondValidation = cache.isSessionValid("session-1", "account-1");
        resolveFirst({ id: "session-1", lastActiveAt: new Date(0) });
        await firstValidation;
        cache.queueSessionUpdate("session-1", 40_000);
        resolveSecond({ id: "session-1", lastActiveAt: new Date(0) });
        await secondValidation;

        await cache.flushPendingUpdates();

        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1);
        expect(mocks.sessionUpdateMany.mock.calls[0][0].data.lastActiveAt)
            .toEqual(new Date(40_000));
    });

    it("drops a missing-record update instead of retrying it forever", async () => {
        mocks.machineFindUnique.mockResolvedValue({
            id: "machine-1",
            lastActiveAt: new Date(0)
        });
        mocks.machineUpdate.mockRejectedValue(
            Object.assign(new Error("record does not exist"), { code: "P2025" })
        );

        await cache.isMachineValid("machine-1", "account-1");
        cache.queueMachineUpdate("machine-1", 40_000);
        await cache.flushPendingUpdates();
        await cache.flushPendingUpdates();

        expect(mocks.machineUpdate).toHaveBeenCalledTimes(1);
    });

    it("awaits an in-flight final flush during shutdown", async () => {
        let resolveUpdate!: (value: { count: number }) => void;
        let didShutdownResolve = false;
        mocks.sessionFindUnique.mockResolvedValue({
            id: "session-1",
            lastActiveAt: new Date(0)
        });
        mocks.sessionUpdateMany.mockImplementationOnce(
            () => new Promise(resolve => { resolveUpdate = resolve; })
        );

        await cache.isSessionValid("session-1", "account-1");
        cache.queueSessionUpdate("session-1", 40_000);
        const shutdown = cache.shutdown().then(() => { didShutdownResolve = true; });
        await vi.waitFor(() => expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(1));
        expect(didShutdownResolve).toBe(false);
        expect(cache.queueSessionUpdate("session-1", 80_000)).toBe(false);

        resolveUpdate({ count: 1 });
        await shutdown;
        expect(didShutdownResolve).toBe(true);
    });

    it("bounds concurrent database writes", async () => {
        let activeWrites = 0;
        let maximumActiveWrites = 0;
        mocks.sessionFindUnique.mockImplementation(async ({ where }: any) => ({
            id: where.id,
            lastActiveAt: new Date(0)
        }));
        mocks.sessionUpdateMany.mockImplementation(async () => {
            activeWrites += 1;
            maximumActiveWrites = Math.max(maximumActiveWrites, activeWrites);
            await new Promise(resolve => setTimeout(resolve, 1));
            activeWrites -= 1;
            return { count: 1 };
        });
        await cache.shutdown();
        cache = new ActivityCache({ autoStart: false, maxFlushConcurrency: 2 });

        for (let index = 0; index < 6; index += 1) {
            const sessionId = `session-${index}`;
            await cache.isSessionValid(sessionId, "account-1");
            cache.queueSessionUpdate(sessionId, 40_000);
        }
        await cache.flushPendingUpdates();

        expect(maximumActiveWrites).toBe(2);
        expect(mocks.sessionUpdateMany).toHaveBeenCalledTimes(6);
    });
});
