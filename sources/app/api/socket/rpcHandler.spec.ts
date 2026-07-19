import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/storage/redis", () => ({
    redis: {
        get: vi.fn(),
        set: vi.fn(),
        eval: vi.fn(),
    },
}));
vi.mock("@/utils/log", () => ({ log: vi.fn() }));

import {
    MAX_RPC_PARAMS_BYTES,
    rpcHandler,
    RpcHandlerLifecycle
} from "@/app/api/socket/rpcHandler";

const CALL_ID = "550e8400-e29b-41d4-a716-446655440000";

function createLifecycle(overrides: Partial<RpcHandlerLifecycle> = {}): RpcHandlerLifecycle {
    return {
        register: vi.fn().mockResolvedValue(undefined),
        unregister: vi.fn().mockResolvedValue(undefined),
        resolve: vi.fn().mockResolvedValue(null),
        close: vi.fn().mockResolvedValue(undefined),
        ...overrides,
    };
}

function createSocket(ackResult: unknown[] | Error = ["encrypted-response"]) {
    const handlers = new Map<string, (...args: any[]) => any>();
    const emitWithAck = ackResult instanceof Error
        ? vi.fn().mockRejectedValue(ackResult)
        : vi.fn().mockResolvedValue(ackResult);
    const timeout = vi.fn(() => ({ emitWithAck }));
    const to = vi.fn(() => ({ timeout }));
    const fetchSockets = vi.fn();
    const socket = {
        id: "caller-socket",
        on: vi.fn((event: string, handler: (...args: any[]) => any) => handlers.set(event, handler)),
        emit: vi.fn(),
        nsp: { to, fetchSockets },
    };
    return { socket, handlers, emitWithAck, timeout, to, fetchSockets };
}

describe("Redis-registered cluster RPC", () => {
    beforeEach(() => vi.restoreAllMocks());

    it("registers and unregisters through the generation-fenced lifecycle", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers } = createSocket();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-register")?.({ method: "machine-1:status" });
        await handlers.get("rpc-unregister")?.({ method: "machine-1:status" });

        expect(lifecycle.register).toHaveBeenCalledWith("machine-1:status");
        expect(lifecycle.unregister).toHaveBeenCalledWith("machine-1:status");
        expect(socket.emit).toHaveBeenCalledWith("rpc-registered", { method: "machine-1:status" });
        expect(socket.emit).toHaveBeenCalledWith("rpc-unregistered", { method: "machine-1:status" });
    });

    it("targets exactly the registered socket ID and forwards a stable optional callId", async () => {
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({ socketId: "target-socket", generation: "generation-1" }),
        });
        const { socket, handlers, emitWithAck, timeout, to, fetchSockets } = createSocket(["encrypted-response"]);
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "machine-1:status",
            params: "encrypted-request",
            callId: CALL_ID,
        }, callback);

        expect(lifecycle.resolve).toHaveBeenCalledWith("machine-1:status");
        expect(fetchSockets).not.toHaveBeenCalled();
        expect(to).toHaveBeenCalledTimes(1);
        expect(to).toHaveBeenCalledWith("target-socket");
        expect(timeout).toHaveBeenCalledWith(30_000);
        expect(emitWithAck).toHaveBeenCalledWith("rpc-request", {
            method: "machine-1:status",
            params: "encrypted-request",
            callId: CALL_ID,
        });
        expect(callback).toHaveBeenCalledWith({
            ok: true,
            result: "encrypted-response",
            callId: CALL_ID,
        });
    });

    it("keeps the legacy request envelope when callId is absent", async () => {
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({ socketId: "target-socket", generation: "generation-1" }),
        });
        const { socket, handlers, emitWithAck } = createSocket(["response"]);
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({ method: "method", params: "request" }, vi.fn());

        expect(emitWithAck).toHaveBeenCalledWith("rpc-request", {
            method: "method",
            params: "request",
        });
    });

    it("rejects a non-UUID callId before resolving or emitting", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers, to } = createSocket();
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "method",
            params: "request",
            callId: "not-a-uuid",
        }, callback);

        expect(lifecycle.resolve).not.toHaveBeenCalled();
        expect(to).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            error: "Invalid parameters: method is required and callId must be a UUID",
        });
    });

    it("rejects oversized RPC parameters before registry resolution or adapter fanout", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers, to } = createSocket();
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "method",
            params: "x".repeat(MAX_RPC_PARAMS_BYTES + 1),
            callId: CALL_ID,
        }, callback);

        expect(lifecycle.resolve).not.toHaveBeenCalled();
        expect(to).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            error: "RPC parameters exceed the size limit",
            callId: CALL_ID,
        });
    });

    it("returns unavailable without adapter fanout when the registry has no target", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers, to } = createSocket();
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({ method: "machine-1:status", params: {} }, callback);

        expect(to).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            error: "RPC method not available",
        });
    });

    it.each([
        ["timeout", new Error("operation has timed out")],
        ["target disconnect", []],
    ])("reports an explicit unknown outcome after one selected-target attempt on %s", async (_label, ackResult) => {
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({ socketId: "target-socket", generation: "generation-1" }),
        });
        const { socket, handlers, to } = createSocket(ackResult);
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "machine-1:status",
            params: {},
            callId: CALL_ID,
        }, callback);

        expect(to).toHaveBeenCalledTimes(1);
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            outcome: "unknown",
            error: "RPC outcome unknown",
            callId: CALL_ID,
        });
    });

    it("compare-deletes this socket's registrations on disconnect", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers } = createSocket();
        rpcHandler("account-1", socket as never, lifecycle);

        handlers.get("disconnect")?.("transport close");

        expect(lifecycle.close).toHaveBeenCalledTimes(1);
    });
});
