import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    sessionFindUnique: vi.fn(),
    sessionUpdateMany: vi.fn(),
    log: vi.fn()
}));

vi.mock("@/storage/db", () => ({
    db: {
        session: {
            findUnique: mocks.sessionFindUnique,
            updateMany: mocks.sessionUpdateMany
        },
        machine: {}
    }
}));
vi.mock("@/utils/log", () => ({ log: mocks.log }));
vi.mock("@/app/monitoring/metrics2", () => ({
    sessionCacheCounter: { inc: vi.fn() },
    databaseUpdatesSkippedCounter: { inc: vi.fn() }
}));

import { ActivityCache, activityCache } from "@/app/presence/sessionCache";

describe("ActivityCache session timestamp fencing", () => {
    let cache: ActivityCache;

    beforeEach(() => {
        mocks.sessionFindUnique.mockReset();
        mocks.sessionUpdateMany.mockReset();
        cache = new ActivityCache();
    });

    afterEach(() => {
        cache.shutdown();
        activityCache.shutdown();
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
});
