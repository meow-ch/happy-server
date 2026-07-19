import { createRpcRegistrationLifecycle } from "@/app/api/socket/rpcRegistrationRegistry";
import type {
    RpcRegistrationLifecycle,
    RpcRegistrationTarget,
} from "@/app/api/socket/rpcRegistrationRegistry";
import { log } from "@/utils/log";
import { Socket } from "socket.io";
import { validate as validateUuid } from "uuid";

const MAX_RPC_METHOD_LENGTH = 512;
export const MAX_RPC_PARAMS_BYTES = 4 * 1024 * 1024;
const RPC_TARGET_ACK_TIMEOUT_MS = 30_000;

function isValidMethod(method: unknown): method is string {
    return typeof method === "string" && method.length > 0 && method.length <= MAX_RPC_METHOD_LENGTH;
}

function isValidCallId(callId: unknown): callId is string | undefined {
    return callId === undefined
        || (typeof callId === "string" && callId.length === 36 && validateUuid(callId));
}

function rpcParamsByteLength(params: unknown): number {
    if (typeof params === "string") return Buffer.byteLength(params, "utf8");
    try {
        return Buffer.byteLength(JSON.stringify(params) ?? "", "utf8");
    } catch {
        return Number.POSITIVE_INFINITY;
    }
}

export type RpcHandlerLifecycle = Pick<
    RpcRegistrationLifecycle,
    "register" | "unregister" | "resolve" | "close"
>;

function withCallId<T extends Record<string, unknown>>(response: T, callId: string | undefined): T & { callId?: string } {
    return callId === undefined ? response : { ...response, callId };
}

function unknownOutcome(callback: ((response: unknown) => void) | undefined, callId: string | undefined): void {
    callback?.(withCallId({
        ok: false,
        outcome: "unknown",
        error: "RPC outcome unknown",
    }, callId));
}

async function emitToSelectedTarget(
    socket: Socket,
    target: RpcRegistrationTarget,
    request: { method: string; params: unknown; callId?: string },
): Promise<unknown> {
    // Every Socket.IO socket automatically owns a room named after its ID. This
    // targets one socket cluster-wide without fetchSockets(), which would copy
    // the remote socket's handshake (including auth data) through the adapter.
    const responses = await socket.nsp
        .to(target.socketId)
        .timeout(RPC_TARGET_ACK_TIMEOUT_MS)
        .emitWithAck("rpc-request", request);

    // The registry identified one target. Zero responses means it disappeared
    // before/during delivery; more than one would violate the socket-ID invariant.
    if (!Array.isArray(responses) || responses.length !== 1) {
        throw new Error("selected RPC target did not return exactly one acknowledgement");
    }
    return responses[0];
}

/**
 * Install RPC handlers and return the lifecycle so startSocket can await its
 * compare-delete cleanup during shutdown if desired. Disconnect cleanup is also
 * installed here; Redis TTL remains the fallback for abrupt process loss.
 */
export function rpcHandler(
    userId: string,
    socket: Socket,
    lifecycle: RpcHandlerLifecycle = createRpcRegistrationLifecycle(userId, socket.id),
): RpcHandlerLifecycle {
    socket.on("rpc-register", async (data: unknown) => {
        try {
            const method = data && typeof data === "object"
                ? (data as Record<string, unknown>).method
                : undefined;
            if (!isValidMethod(method)) {
                socket.emit("rpc-error", { type: "register", error: "Invalid method name" });
                return;
            }

            await lifecycle.register(method);
            socket.emit("rpc-registered", { method });
        } catch (error) {
            log({ module: "websocket", level: "error" }, `Error in rpc-register: ${error}`);
            socket.emit("rpc-error", { type: "register", error: "Internal error" });
        }
    });

    socket.on("rpc-unregister", async (data: unknown) => {
        try {
            const method = data && typeof data === "object"
                ? (data as Record<string, unknown>).method
                : undefined;
            if (!isValidMethod(method)) {
                socket.emit("rpc-error", { type: "unregister", error: "Invalid method name" });
                return;
            }

            await lifecycle.unregister(method);
            socket.emit("rpc-unregistered", { method });
        } catch (error) {
            log({ module: "websocket", level: "error" }, `Error in rpc-unregister: ${error}`);
            socket.emit("rpc-error", { type: "unregister", error: "Internal error" });
        }
    });

    socket.on("rpc-call", async (data: unknown, callback?: (response: unknown) => void) => {
        const request = data && typeof data === "object"
            ? data as Record<string, unknown>
            : {};
        const method = request.method;
        const callId = request.callId;

        if (!isValidMethod(method) || !isValidCallId(callId)) {
            callback?.(withCallId({
                ok: false,
                error: "Invalid parameters: method is required and callId must be a UUID",
            }, typeof callId === "string" && callId.length === 36 && validateUuid(callId) ? callId : undefined));
            return;
        }
        if (rpcParamsByteLength(request.params) > MAX_RPC_PARAMS_BYTES) {
            callback?.(withCallId({
                ok: false,
                error: "RPC parameters exceed the size limit",
            }, callId));
            return;
        }

        let target: RpcRegistrationTarget | null;
        try {
            target = await lifecycle.resolve(method);
        } catch (error) {
            log({ module: "websocket-rpc", level: "error" }, `Failed to resolve RPC method ${method}: ${error}`);
            callback?.(withCallId({
                ok: false,
                error: "RPC registry unavailable",
            }, callId));
            return;
        }

        if (!target) {
            callback?.(withCallId({
                ok: false,
                error: "RPC method not available",
            }, callId));
            return;
        }
        if (target.socketId === socket.id) {
            callback?.(withCallId({
                ok: false,
                error: "Cannot call RPC on the same socket",
            }, callId));
            return;
        }

        const forwardedRequest = {
            method,
            params: request.params,
            ...(callId === undefined ? {} : { callId }),
        };

        try {
            const response = await emitToSelectedTarget(socket, target, forwardedRequest);
            callback?.(withCallId({
                ok: true,
                result: response,
            }, callId));
        } catch (error) {
            // Once the one selected target has been attempted, timeout or
            // disconnect cannot prove whether it performed the operation. Do
            // not fail over and risk executing a side effect twice.
            log({ module: "websocket-rpc", level: "warn" }, `RPC outcome unknown for ${method}: ${error}`);
            unknownOutcome(callback, callId);
        }
    });

    socket.on("disconnect", () => {
        void lifecycle.close();
    });

    return lifecycle;
}
