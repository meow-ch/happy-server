import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ constructor: vi.fn() }));

vi.mock("ioredis", () => ({
    Redis: class {
        constructor(url: string, options?: Record<string, unknown>) {
            mocks.constructor(url, options);
        }
    },
}));

import { RPC_REGISTRATION_REDIS_OPTIONS } from "@/storage/redis";

describe("RPC registration Redis transport", () => {
    it("is fail-fast and never queues or auto-resends fenced commands", () => {
        expect(mocks.constructor).toHaveBeenCalledTimes(2);
        expect(mocks.constructor.mock.calls[1][1]).toBe(RPC_REGISTRATION_REDIS_OPTIONS);
        expect(RPC_REGISTRATION_REDIS_OPTIONS).toMatchObject({
            lazyConnect: true,
            enableOfflineQueue: false,
            autoResendUnfulfilledCommands: false,
            commandTimeout: 5_000,
            connectTimeout: 5_000,
            maxRetriesPerRequest: 1,
        });
        expect(RPC_REGISTRATION_REDIS_OPTIONS.retryStrategy(1)).toBe(100);
        expect(RPC_REGISTRATION_REDIS_OPTIONS.retryStrategy(100)).toBe(1_000);
    });
});
