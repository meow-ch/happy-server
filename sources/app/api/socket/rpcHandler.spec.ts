import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    runWithRuntimeConnectionOwnerLock: vi.fn(),
    isRuntimeConnectionOwner: vi.fn(),
}));

vi.mock("@/storage/redis", () => ({
    rpcRegistrationRedis: {
        get: vi.fn(),
        set: vi.fn(),
        eval: vi.fn(),
    },
}));
vi.mock("@/utils/log", () => ({ log: vi.fn() }));
vi.mock("@/app/presence/runtimeConnectionLease", () => ({
    runWithRuntimeConnectionOwnerLock: mocks.runWithRuntimeConnectionOwnerLock,
    isRuntimeConnectionOwner: mocks.isRuntimeConnectionOwner,
}));

import {
    MAX_RPC_PARAMS_BYTES,
    rpcHandler,
    RpcHandlerLifecycle
} from "@/app/api/socket/rpcHandler";
import {
    RpcRegistrationOwnershipLostError,
} from "@/app/api/socket/rpcRegistrationRegistry";

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
        disconnect: vi.fn(),
        on: vi.fn((event: string, handler: (...args: any[]) => any) => handlers.set(event, handler)),
        emit: vi.fn(),
        nsp: { to, fetchSockets, sockets: new Map<string, unknown>() },
    };
    return { socket, handlers, emitWithAck, timeout, to, fetchSockets };
}

function installLocalTarget(
    socket: ReturnType<typeof createSocket>["socket"],
    ack: unknown[] | Promise<unknown>,
) {
    const emitWithAck = ack instanceof Promise
        ? vi.fn(() => ack)
        : vi.fn().mockResolvedValue(ack[0]);
    const timeout = vi.fn(() => ({ emitWithAck }));
    socket.nsp.sockets.set("target-socket", { timeout });
    return { emitWithAck, timeout };
}

describe("Redis-registered cluster RPC", () => {
    beforeEach(() => {
        vi.restoreAllMocks();
        mocks.runWithRuntimeConnectionOwnerLock.mockReset();
        mocks.isRuntimeConnectionOwner.mockReset();
    });

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

    it("delegates session RPC registration to the ownership-fenced lifecycle", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers } = createSocket();
        const connection = {
            connectionType: "session-scoped",
            socket,
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
            runtimeConnectionLeaseId: "lease-1",
        };
        rpcHandler("account-1", socket as never, lifecycle, connection as never);

        await handlers.get("rpc-register")?.({ method: "session-1:status" });

        expect(lifecycle.register).toHaveBeenCalledWith("session-1:status");
        expect(mocks.runWithRuntimeConnectionOwnerLock).not.toHaveBeenCalled();
        expect(socket.emit).toHaveBeenCalledWith("rpc-registered", { method: "session-1:status" });
    });

    it("cannot register after a successor wins the session lease", async () => {
        const lifecycle = createLifecycle({
            register: vi.fn().mockRejectedValue(new RpcRegistrationOwnershipLostError()),
        });
        const { socket, handlers } = createSocket();
        const connection = {
            connectionType: "session-scoped",
            socket,
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
            runtimeConnectionLeaseId: "old-lease",
        };
        rpcHandler("account-1", socket as never, lifecycle, connection as never);

        await handlers.get("rpc-register")?.({ method: "session-1:status" });

        expect(lifecycle.register).toHaveBeenCalledWith("session-1:status");
        expect(socket.disconnect).toHaveBeenCalledWith(true);
        expect(socket.emit).toHaveBeenCalledWith("rpc-error", {
            type: "register",
            error: "Runtime connection is stale",
        });
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

    it("executes one owner-fenced RPC and rejects a concurrent one as retryable not-started", async () => {
        let releaseFirst!: (value: unknown) => void;
        let ownerBusy = false;
        mocks.runWithRuntimeConnectionOwnerLock.mockImplementation(async (_owner, operation) => {
            if (ownerBusy) return "busy";
            ownerBusy = true;
            try {
                await operation();
                return "completed";
            } finally {
                ownerBusy = false;
            }
        });
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({
                socketId: "target-socket",
                generation: "generation-1",
                runtimeOwner: {
                    sessionId: "session-1",
                    sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
                    leaseId: "lease-1",
                },
            }),
        });
        const { socket, handlers, to } = createSocket();
        const deferredAck = new Promise<unknown>(resolve => { releaseFirst = resolve; });
        const localTarget = installLocalTarget(socket, deferredAck);
        const firstCallback = vi.fn();
        const secondCallback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        const first = handlers.get("rpc-call")?.({
            method: "session-1:status",
            params: "first",
            callId: CALL_ID,
        }, firstCallback);
        await vi.waitFor(() => expect(localTarget.emitWithAck).toHaveBeenCalledTimes(1));
        await handlers.get("rpc-call")?.({
            method: "session-1:status",
            params: "second",
            callId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        }, secondCallback);

        expect(localTarget.emitWithAck).toHaveBeenCalledTimes(1);
        expect(to).not.toHaveBeenCalled();
        expect(secondCallback).toHaveBeenCalledWith({
            ok: false,
            outcome: "not_started",
            retryable: true,
            error: "RPC target is busy",
            callId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        });

        releaseFirst("first-response");
        await first;
        expect(firstCallback).toHaveBeenCalledWith({
            ok: true,
            result: "first-response",
            callId: CALL_ID,
        });
    });

    it("fails owner RPC as known-not-started when its exact socket is not local", async () => {
        mocks.runWithRuntimeConnectionOwnerLock.mockImplementation(async (_owner, operation) => {
            await operation();
            return "completed";
        });
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({
                socketId: "target-socket",
                generation: "generation-1",
                runtimeOwner: {
                    sessionId: "session-1",
                    sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
                    leaseId: "lease-1",
                },
            }),
        });
        const { socket, handlers, emitWithAck, to } = createSocket();
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "session-1:status",
            params: {},
            callId: CALL_ID,
        }, callback);

        expect(to).not.toHaveBeenCalled();
        expect(emitWithAck).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            outcome: "not_started",
            retryable: true,
            error: "RPC method not available",
            callId: CALL_ID,
        });
    });

    it("does not emit when the registered RPC target no longer owns its session", async () => {
        mocks.runWithRuntimeConnectionOwnerLock.mockResolvedValue("not_owner");
        const lifecycle = createLifecycle({
            resolve: vi.fn().mockResolvedValue({
                socketId: "target-socket",
                generation: "generation-1",
                runtimeOwner: {
                    sessionId: "session-1",
                    sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
                    leaseId: "stale-lease",
                },
            }),
        });
        const { socket, handlers, emitWithAck } = createSocket();
        const callback = vi.fn();
        rpcHandler("account-1", socket as never, lifecycle);

        await handlers.get("rpc-call")?.({
            method: "session-1:status",
            params: {},
            callId: CALL_ID,
        }, callback);

        expect(emitWithAck).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            error: "RPC method not available",
            callId: CALL_ID,
        });
    });

    it("rejects rpc-call from a session runtime", async () => {
        const lifecycle = createLifecycle();
        const { socket, handlers, to } = createSocket();
        const callback = vi.fn();
        const connection = {
            connectionType: "session-scoped",
            socket,
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
            runtimeConnectionLeaseId: "lease-1",
        };
        rpcHandler("account-1", socket as never, lifecycle, connection as never);

        await handlers.get("rpc-call")?.({ method: "method", params: "request" }, callback);

        expect(to).not.toHaveBeenCalled();
        expect(socket.disconnect).toHaveBeenCalledWith(true);
        expect(callback).toHaveBeenCalledWith({
            ok: false,
            error: "Session runtimes cannot initiate RPC calls",
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
