import type { ClientConnection, SessionScopedConnection } from "@/app/events/eventRouter";
import { isRuntimeConnectionOwner } from "@/app/presence/runtimeConnectionLease";
import { log } from "@/utils/log";

const REPLAY_ONLY_EVENTS = new Set(["message", "session-end", "ping"]);
const LIVE_SESSION_EVENTS = new Set([
    "message",
    "session-end",
    "session-alive",
    "update-metadata",
    "update-state",
    "usage-report",
    "ping",
    "rpc-register",
    "rpc-unregister",
]);
const SELF_FENCED_EVENTS = new Set(["session-alive", "session-end"]);

export function rejectRuntimeConnection(
    connection: SessionScopedConnection,
    reason: string,
): void {
    if (connection.runtimeConnectionLeaseRejected) return;
    connection.runtimeConnectionLeaseRejected = true;
    log(
        { module: "websocket", level: "warn" },
        `Disconnecting rejected runtime socket sessionId=${connection.sessionId} socketId=${connection.socket.id} reason=${reason}`,
    );
    connection.socket.disconnect(true);
}

/**
 * Fences every session-scoped packet before its handler. The critical message
 * write repeats this check under the session-row lock to close the claim race;
 * session-alive and session-end have their own exact atomic transitions.
 */
export function installRuntimeConnectionPacketFence(connection: ClientConnection): void {
    if (connection.connectionType !== "session-scoped") return;

    connection.socket.use(async (packet, next) => {
        const eventName = typeof packet[0] === "string" ? packet[0] : "";
        if (connection.runtimeConnectionLeaseRejected) {
            next(new Error("Runtime connection is stale"));
            return;
        }

        if (connection.replayOnly) {
            if (REPLAY_ONLY_EVENTS.has(eventName)) {
                next();
                return;
            }
            rejectRuntimeConnection(connection, `replay-only event ${eventName}`);
            next(new Error("Replay transport cannot perform live runtime operations"));
            return;
        }

        if (!LIVE_SESSION_EVENTS.has(eventName)) {
            rejectRuntimeConnection(connection, `cross-scope event ${eventName}`);
            next(new Error("Session runtime cannot perform this operation"));
            return;
        }

        if (SELF_FENCED_EVENTS.has(eventName) || eventName === "ping") {
            next();
            return;
        }

        try {
            const owned = await isRuntimeConnectionOwner({
                accountId: connection.userId,
                sessionId: connection.sessionId,
                sessionInstanceId: connection.sessionInstanceId,
                leaseId: connection.runtimeConnectionLeaseId,
            });
            if (!owned) {
                rejectRuntimeConnection(connection, `lease check failed for ${eventName}`);
                next(new Error("Runtime connection is stale"));
                return;
            }
            next();
        } catch (error) {
            log(
                { module: "websocket", level: "error" },
                `Runtime lease authorization failed for session ${connection.sessionId}: ${error}`,
            );
            next(new Error("Runtime authorization unavailable"));
        }
    });
}
