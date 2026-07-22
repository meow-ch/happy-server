import { describe, expect, it, vi } from "vitest";

vi.mock("@/storage/redis", () => ({
    redis: {
        get: vi.fn(),
        set: vi.fn(),
        eval: vi.fn(),
    },
}));
vi.mock("@/utils/log", () => ({ log: vi.fn() }));
vi.mock("@/app/monitoring/metrics2", () => ({
    rpcRegistrationRefreshCounter: { inc: vi.fn() },
    rpcRegistrationRefreshBatchSizeHistogram: { observe: vi.fn() },
}));

import {
    getRpcRegistrationKey,
    RedisRpcRegistrationRegistry,
    RpcRegistrationLifecycle,
} from "@/app/api/socket/rpcRegistrationRegistry";

class FakeRedis {
    readonly values = new Map<string, string>();
    readonly ttlByKey = new Map<string, number>();
    readonly set = vi.fn(async (key: string, value: string, mode: string, ttlMs: number) => {
        expect(mode).toBe("PX");
        this.values.set(key, value);
        this.ttlByKey.set(key, ttlMs);
        return "OK";
    });
    readonly get = vi.fn(async (key: string) => this.values.get(key) ?? null);
    readonly eval = vi.fn(async (
        _script: string,
        numberOfKeys: number,
        ...args: Array<string | number>
    ) => {
        const keys = args.slice(0, numberOfKeys) as string[];
        const expectedValues = args.slice(numberOfKeys, numberOfKeys * 2) as string[];
        const ttlMs = args[numberOfKeys * 2] as number | undefined;
        return keys.map((key, index) => {
            const expectedValue = expectedValues[index];
            if (ttlMs !== undefined) {
                const current = this.values.get(key);
                if (current !== undefined && current !== expectedValue) {
                    const currentGeneration = JSON.parse(current).generation;
                    const expectedGeneration = JSON.parse(expectedValue).generation;
                    if (currentGeneration >= expectedGeneration) return 0;
                }
                this.values.set(key, expectedValue);
                this.ttlByKey.set(key, ttlMs);
                return 1;
            }
            if (this.values.get(key) !== expectedValue) return 0;
            this.values.delete(key);
            this.ttlByKey.delete(key);
            return 1;
        });
    });
}

describe("RedisRpcRegistrationRegistry", () => {
    it("stores only the socket ID and registration generation under an account-method key", async () => {
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never, 45_000);

        const registration = await registry.register("account-1", "machine-1:status", "socket-1");
        const key = getRpcRegistrationKey("account-1", "machine-1:status");
        const stored = JSON.parse(redis.values.get(key)!);

        expect(key).toMatch(/^happy:rpc-registration:v1:[A-Za-z0-9_-]{43}$/);
        expect(getRpcRegistrationKey("account-2", "machine-1:status")).not.toBe(key);
        expect(getRpcRegistrationKey("account-1", "machine-2:status")).not.toBe(key);
        expect(registration.key).toBe(key);
        expect(registration.generation).toMatch(/^\d{16}:\d{8}:[0-9a-f-]{36}$/);
        expect(stored).toEqual({
            socketId: "socket-1",
            generation: registration.generation,
        });
        expect(Object.keys(stored).sort()).toEqual(["generation", "socketId"]);
        expect(redis.ttlByKey.get(key)).toBe(45_000);
        await expect(registry.resolve("account-1", "machine-1:status")).resolves.toEqual({
            socketId: "socket-1",
            generation: registration.generation,
        });
    });

    it("does not let a stale refresh or disconnect mutate a newer registration", async () => {
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never, 45_000);
        const first = await registry.register("account-1", "machine-1:status", "socket-old");
        const current = await registry.register("account-1", "machine-1:status", "socket-new");

        await expect(registry.refresh(first)).resolves.toBe(false);
        await expect(registry.unregister(first)).resolves.toBe(false);
        await expect(registry.resolve("account-1", "machine-1:status")).resolves.toEqual({
            socketId: "socket-new",
            generation: current.generation,
        });

        await expect(registry.refresh(current)).resolves.toBe(true);
        await expect(registry.unregister(current)).resolves.toBe(true);
        await expect(registry.resolve("account-1", "machine-1:status")).resolves.toBeNull();
    });

    it("converges to the freshest generation after Redis loss regardless of reclaim order", async () => {
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never, 45_000);
        const now = vi.spyOn(Date, "now");
        try {
            now.mockReturnValueOnce(1_000);
            const stale = await registry.register("account-1", "machine-1:status", "socket-1");
            now.mockReturnValueOnce(2_000);
            const freshest = await registry.register("account-1", "machine-1:status", "socket-2");

            redis.values.clear();
            redis.ttlByKey.clear();
            await expect(registry.refresh(stale)).resolves.toBe(true);
            await expect(registry.resolve("account-1", "machine-1:status")).resolves.toEqual({
                socketId: "socket-1",
                generation: stale.generation,
            });

            await expect(registry.refresh(freshest)).resolves.toBe(true);
            await expect(registry.resolve("account-1", "machine-1:status")).resolves.toEqual({
                socketId: "socket-2",
                generation: freshest.generation,
            });
            expect(redis.ttlByKey.get(freshest.key)).toBe(45_000);

            await expect(registry.refresh(stale)).resolves.toBe(false);
        } finally {
            now.mockRestore();
        }
    });

    it("refreshes registrations while connected and compare-deletes them on close", async () => {
        vi.useFakeTimers();
        try {
            const redis = new FakeRedis();
            const registry = new RedisRpcRegistrationRegistry(redis as never, 60);
            const lifecycle = new RpcRegistrationLifecycle(
                "account-1",
                "socket-1",
                registry,
                20,
            );

            await lifecycle.register("machine-1:status");
            expect(redis.eval).not.toHaveBeenCalled();

            await vi.advanceTimersByTimeAsync(20);
            expect(redis.eval).toHaveBeenCalledTimes(1);

            await lifecycle.close();
            expect(redis.eval).toHaveBeenCalledTimes(2);
            await expect(registry.resolve("account-1", "machine-1:status")).resolves.toBeNull();

            await vi.advanceTimersByTimeAsync(100);
            expect(redis.eval).toHaveBeenCalledTimes(2);
        } finally {
            vi.useRealTimers();
        }
    });

    it("refreshes and closes every method for a socket with one Redis command per operation", async () => {
        vi.useFakeTimers();
        try {
            const redis = new FakeRedis();
            const registry = new RedisRpcRegistrationRegistry(redis as never, 60);
            const lifecycle = new RpcRegistrationLifecycle(
                "account-1",
                "socket-1",
                registry,
                20,
            );

            await lifecycle.register("method-1");
            await lifecycle.register("method-2");
            await lifecycle.register("method-3");
            await vi.advanceTimersByTimeAsync(20);

            expect(redis.eval).toHaveBeenCalledTimes(1);
            expect(redis.eval.mock.calls[0][1]).toBe(3);
            expect([...redis.ttlByKey.values()]).toEqual([60, 60, 60]);

            await lifecycle.close();
            expect(redis.eval).toHaveBeenCalledTimes(2);
            expect(redis.eval.mock.calls[1][1]).toBe(3);
            expect(redis.values.size).toBe(0);
        } finally {
            vi.useRealTimers();
        }
    });

    it("coalesces overlapping refresh ticks while Redis is slow", async () => {
        let resolveRefresh!: (value: Array<0 | 1>) => void;
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never, 60_000);
        const lifecycle = new RpcRegistrationLifecycle(
            "account-1",
            "socket-1",
            registry,
            20_000,
        );
        await lifecycle.register("method-1");
        await lifecycle.register("method-2");
        redis.eval.mockImplementationOnce(() => new Promise(resolve => { resolveRefresh = resolve; }));

        const firstRefresh = lifecycle.refresh();
        const overlappingRefresh = lifecycle.refresh();
        expect(overlappingRefresh).toBe(firstRefresh);
        await vi.waitFor(() => expect(redis.eval).toHaveBeenCalledTimes(1));

        resolveRefresh([1, 1]);
        await firstRefresh;
        await lifecycle.close();
    });

    it("treats malformed registry values as unavailable", async () => {
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never);
        redis.values.set(getRpcRegistrationKey("account-1", "method"), JSON.stringify({ socketId: 123 }));

        await expect(registry.resolve("account-1", "method")).resolves.toBeNull();
    });

    it("bounds registrations retained and refreshed by one socket", async () => {
        const redis = new FakeRedis();
        const registry = new RedisRpcRegistrationRegistry(redis as never);
        const lifecycle = new RpcRegistrationLifecycle(
            "account-1",
            "socket-1",
            registry,
            20_000,
            1,
        );

        await lifecycle.register("method-1");
        await expect(lifecycle.register("method-2")).rejects.toThrow("registration limit");
        await lifecycle.close();
    });
});
