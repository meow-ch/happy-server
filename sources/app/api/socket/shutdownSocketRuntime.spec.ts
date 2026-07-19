import { describe, expect, it, vi } from "vitest";
import { shutdownSocketRuntime } from "@/app/api/socket/shutdownSocketRuntime";

describe("shutdownSocketRuntime", () => {
    it("closes sockets and RPC registrations before stopping notification delivery", async () => {
        const order: string[] = [];
        const step = (name: string) => vi.fn(async () => { order.push(name); });
        const ioClose = step("io.close");
        const rpcClose = step("rpc.close");
        const dispatcherStop = step("dispatcher.stop");
        const busStop = step("bus.stop");
        const socketRedisQuit = step("socket-redis.quit");
        const notificationRedisQuit = step("notification-redis.quit");

        await shutdownSocketRuntime({
            io: { close: ioClose },
            rpcLifecycles: [{ close: rpcClose }],
            notificationDispatcher: { stop: dispatcherStop },
            notificationBus: { stop: busStop },
            redisClients: [
                { quit: socketRedisQuit },
                { quit: notificationRedisQuit },
            ],
        });

        expect(order).toEqual([
            "io.close",
            "rpc.close",
            "dispatcher.stop",
            "bus.stop",
            "socket-redis.quit",
            "notification-redis.quit",
        ]);
    });

    it("waits for RPC compare-delete cleanup before stopping the dispatcher", async () => {
        let releaseRpc!: () => void;
        const rpcClosed = new Promise<void>((resolve) => { releaseRpc = resolve; });
        const dispatcherStop = vi.fn().mockResolvedValue(undefined);
        const shutdown = shutdownSocketRuntime({
            io: { close: vi.fn().mockResolvedValue(undefined) },
            rpcLifecycles: [{ close: vi.fn(() => rpcClosed) }],
            notificationDispatcher: { stop: dispatcherStop },
            notificationBus: { stop: vi.fn().mockResolvedValue(undefined) },
            redisClients: [],
        });

        await Promise.resolve();
        await Promise.resolve();
        expect(dispatcherStop).not.toHaveBeenCalled();

        releaseRpc();
        await shutdown;
        expect(dispatcherStop).toHaveBeenCalledTimes(1);
    });

    it("bounds a stalled Redis quit and forces a disconnect", async () => {
        vi.useFakeTimers();
        try {
            const disconnect = vi.fn();
            const shutdown = shutdownSocketRuntime({
                io: { close: vi.fn().mockResolvedValue(undefined) },
                rpcLifecycles: [],
                notificationDispatcher: { stop: vi.fn().mockResolvedValue(undefined) },
                notificationBus: { stop: vi.fn().mockResolvedValue(undefined) },
                redisClients: [{
                    quit: vi.fn(() => new Promise(() => {})),
                    disconnect,
                }],
                phaseTimeoutMs: 25,
                redisQuitTimeoutMs: 25,
            });
            const rejected = expect(shutdown).rejects.toThrow("Redis client 1 quit: timed out");

            await vi.advanceTimersByTimeAsync(25);
            await rejected;
            expect(disconnect).toHaveBeenCalledTimes(1);
        } finally {
            vi.useRealTimers();
        }
    });
});
