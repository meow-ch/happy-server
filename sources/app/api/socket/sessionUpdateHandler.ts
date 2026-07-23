import { sessionAliveEventsCounter, websocketEventsCounter } from "@/app/monitoring/metrics2";
import { buildUpdateSessionUpdate, ClientConnection, eventRouter } from "@/app/events/eventRouter";
import { sessionActivityPublisher } from "@/app/presence/sessionActivityPublisher";
import { db } from "@/storage/db";
import { allocateUserSeq } from "@/storage/seq";
import { AsyncLock } from "@/utils/lock";
import { log } from "@/utils/log";
import { randomKeyNaked } from "@/utils/randomKeyNaked";
import { Socket } from "socket.io";
import { persistSessionMessage, SessionMessageAck, toSessionMessageAck } from "@/app/session/sessionMessageCreate";
import { SessionMessageNotificationDispatcher } from "@/app/session/sessionMessageNotificationOutbox";
import { parseSessionMessageLocalId } from "@/app/api/socket/sessionMessageValidation";
import {
    endRuntimeConnectionLease,
    isRuntimeConnectionOwner,
    newRuntimeConnectionLeaseTombstone,
    renewLegacyRuntimeConnection,
    renewRuntimeConnectionLease,
    updateRuntimeSessionMetadata,
    updateRuntimeSessionState,
} from "@/app/presence/runtimeConnectionLease";
import { rejectRuntimeConnection } from "@/app/api/socket/runtimeConnectionGuard";

type SessionEndAck =
    | { result: "success"; localId: string | null }
    | { result: "error"; code: "invalid_request" | "session_not_found" | "internal_error"; retryable: boolean };

export const MAX_SESSION_MESSAGE_CIPHERTEXT_BYTES = 7 * 1024 * 1024;

function runtimeWriteAuthorization(
    connection: ClientConnection,
    sessionId: string,
): { sessionInstanceId?: string; leaseId?: string } | null {
    if (connection.connectionType !== "session-scoped") return {};
    if (connection.replayOnly || connection.sessionId !== sessionId) return null;
    if (connection.sessionInstanceId && connection.runtimeConnectionLeaseId) {
        return {
            sessionInstanceId: connection.sessionInstanceId,
            leaseId: connection.runtimeConnectionLeaseId,
        };
    }
    if (!connection.sessionInstanceId && !connection.runtimeConnectionLeaseId) {
        return {};
    }
    return null;
}

async function stillOwnsRuntimeConnection(
    userId: string,
    connection: ClientConnection,
    sessionId: string,
): Promise<boolean> {
    if (connection.connectionType !== "session-scoped") return true;
    return isRuntimeConnectionOwner({
        accountId: userId,
        sessionId,
        sessionInstanceId: connection.sessionInstanceId,
        leaseId: connection.runtimeConnectionLeaseId,
    });
}

export function sessionUpdateHandler(
    userId: string,
    socket: Socket,
    connection: ClientConnection,
    notificationDispatcher: SessionMessageNotificationDispatcher
) {
    socket.on('update-metadata', async (data: any, callback: (response: any) => void) => {
        try {
            const { sid, metadata, expectedVersion } = data;

            // Validate input
            if (!sid || typeof metadata !== 'string' || typeof expectedVersion !== 'number') {
                if (callback) {
                    callback({ result: 'error' });
                }
                return;
            }
            const runtimeAuthorization = runtimeWriteAuthorization(connection, sid);
            if (runtimeAuthorization === null) {
                callback?.({ result: 'error' });
                if (connection.connectionType === "session-scoped") {
                    rejectRuntimeConnection(connection, "metadata write fence rejected");
                }
                return;
            }

            // Resolve session
            const session = await db.session.findUnique({
                where: { id: sid, accountId: userId }
            });
            if (!session) {
                return;
            }

            // Check version
            if (session.metadataVersion !== expectedVersion) {
                callback({ result: 'version-mismatch', version: session.metadataVersion, metadata: session.metadata });
                return null;
            }

            // Update metadata
            const count = connection.connectionType === "session-scoped"
                ? await updateRuntimeSessionMetadata({
                    accountId: userId,
                    sessionId: sid,
                    ...runtimeAuthorization,
                    expectedVersion,
                    metadata,
                }).then(updated => updated ? 1 : 0)
                : (await db.session.updateMany({
                    where: { id: sid, accountId: userId, metadataVersion: expectedVersion },
                    data: {
                        metadata,
                        metadataVersion: expectedVersion + 1,
                    },
                })).count;
            if (count === 0) {
                if (connection.connectionType === "session-scoped") {
                    if (!await stillOwnsRuntimeConnection(userId, connection, sid)) {
                        rejectRuntimeConnection(connection, "metadata write lease CAS lost");
                        callback?.({ result: 'error' });
                        return null;
                    }
                    const current = await db.session.findUnique({
                        where: { id: sid, accountId: userId },
                    });
                    callback({
                        result: 'version-mismatch',
                        version: current?.metadataVersion ?? session.metadataVersion,
                        metadata: current?.metadata ?? session.metadata,
                    });
                    return null;
                }
                callback({ result: 'version-mismatch', version: session.metadataVersion, metadata: session.metadata });
                return null;
            }

            // Generate session metadata update
            const updSeq = await allocateUserSeq(userId);
            const metadataUpdate = {
                value: metadata,
                version: expectedVersion + 1
            };
            const updatePayload = buildUpdateSessionUpdate(sid, updSeq, randomKeyNaked(12), metadataUpdate);
            eventRouter.emitUpdate({
                userId,
                payload: updatePayload,
                recipientFilter: { type: 'all-interested-in-session', sessionId: sid }
            });

            // Send success response with new version via callback
            callback({ result: 'success', version: expectedVersion + 1, metadata: metadata });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in update-metadata: ${error}`);
            if (callback) {
                callback({ result: 'error' });
            }
        }
    });

    socket.on('update-state', async (data: any, callback: (response: any) => void) => {
        try {
            const { sid, agentState, expectedVersion } = data;

            // Validate input
            if (!sid || (typeof agentState !== 'string' && agentState !== null) || typeof expectedVersion !== 'number') {
                if (callback) {
                    callback({ result: 'error' });
                }
                return;
            }
            const runtimeAuthorization = runtimeWriteAuthorization(connection, sid);
            if (runtimeAuthorization === null) {
                callback?.({ result: 'error' });
                if (connection.connectionType === "session-scoped") {
                    rejectRuntimeConnection(connection, "state write fence rejected");
                }
                return;
            }

            // Resolve session
            const session = await db.session.findUnique({
                where: {
                    id: sid,
                    accountId: userId
                }
            });
            if (!session) {
                callback({ result: 'error' });
                return null;
            }

            // Check version
            if (session.agentStateVersion !== expectedVersion) {
                callback({ result: 'version-mismatch', version: session.agentStateVersion, agentState: session.agentState });
                return null;
            }

            // Update agent state
            const count = connection.connectionType === "session-scoped"
                ? await updateRuntimeSessionState({
                    accountId: userId,
                    sessionId: sid,
                    ...runtimeAuthorization,
                    expectedVersion,
                    agentState,
                }).then(updated => updated ? 1 : 0)
                : (await db.session.updateMany({
                    where: { id: sid, accountId: userId, agentStateVersion: expectedVersion },
                    data: {
                        agentState,
                        agentStateVersion: expectedVersion + 1,
                    },
                })).count;
            if (count === 0) {
                if (connection.connectionType === "session-scoped") {
                    if (!await stillOwnsRuntimeConnection(userId, connection, sid)) {
                        rejectRuntimeConnection(connection, "state write lease CAS lost");
                        callback?.({ result: 'error' });
                        return null;
                    }
                    const current = await db.session.findUnique({
                        where: { id: sid, accountId: userId },
                    });
                    callback({
                        result: 'version-mismatch',
                        version: current?.agentStateVersion ?? session.agentStateVersion,
                        agentState: current?.agentState ?? session.agentState,
                    });
                    return null;
                }
                callback({ result: 'version-mismatch', version: session.agentStateVersion, agentState: session.agentState });
                return null;
            }

            // Generate session agent state update
            const updSeq = await allocateUserSeq(userId);
            const agentStateUpdate = {
                value: agentState,
                version: expectedVersion + 1
            };
            const updatePayload = buildUpdateSessionUpdate(sid, updSeq, randomKeyNaked(12), undefined, agentStateUpdate);
            eventRouter.emitUpdate({
                userId,
                payload: updatePayload,
                recipientFilter: { type: 'all-interested-in-session', sessionId: sid }
            });

            // Send success response with new version via callback
            callback({ result: 'success', version: expectedVersion + 1, agentState: agentState });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in update-state: ${error}`);
            if (callback) {
                callback({ result: 'error' });
            }
        }
    });
    socket.on('session-alive', async (data: {
        sid: string;
        time: number;
        thinking?: boolean;
    }) => {
        try {
            // Track metrics
            websocketEventsCounter.inc({ event_type: 'session-alive' });
            sessionAliveEventsCounter.inc();

            // Basic validation
            if (!data || typeof data.time !== 'number' || !data.sid) {
                return;
            }
            if (connection.connectionType !== 'session-scoped'
                || connection.replayOnly
                || data.sid !== connection.sessionId) {
                return;
            }
            const modernConnection = connection.sessionInstanceId
                && connection.runtimeConnectionLeaseId;
            const legacyConnection = !connection.sessionInstanceId
                && !connection.runtimeConnectionLeaseId;
            if (!modernConnection && !legacyConnection) return;

            const { sid, thinking } = data;
            // Client time is display metadata, never lease authority. Use the
            // server receipt clock for batching; the persisted renewal itself
            // uses PostgreSQL CURRENT_TIMESTAMP.
            const receivedAt = Date.now();

            if (connection.runtimeConnectionLeaseRejected) return;
            if (modernConnection) {
                const sessionInstanceId = connection.sessionInstanceId;
                const leaseId = connection.runtimeConnectionLeaseId;
                if (!sessionInstanceId || !leaseId) return;
                // Renew every heartbeat directly. A shared coalescing cache
                // cannot safely watermark two competing socket generations:
                // a stale generation must observe its own failed exact CAS.
                const renewed = await renewRuntimeConnectionLease({
                    accountId: userId,
                    sessionId: sid,
                    sessionInstanceId,
                    leaseId,
                });
                if (!renewed) {
                    rejectRuntimeConnection(connection, "heartbeat lease CAS failed");
                    return;
                }
            } else {
                // Pre-incarnation clients can update only an untouched legacy
                // row. They cannot revive or impersonate a managed runtime.
                const renewed = await renewLegacyRuntimeConnection({
                    accountId: userId,
                    sessionId: sid,
                });
                if (!renewed) {
                    rejectRuntimeConnection(connection, "legacy heartbeat CAS failed");
                    return;
                }
            }

            // Coalesce unchanged heartbeats before cross-node Redis fanout.
            sessionActivityPublisher.publish({
                userId,
                sessionId: sid,
                active: true,
                activeAt: receivedAt,
                thinking: thinking || false,
            });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in session-alive: ${error}`);
        }
    });

    const receiveMessageLock = new AsyncLock();
    socket.on('message', async (data: any, callback?: (response: SessionMessageAck) => void) => {
        await receiveMessageLock.inLock(async () => {
            websocketEventsCounter.inc({ event_type: 'message' });
            if (!data || typeof data.sid !== 'string' || data.sid.length === 0 || typeof data.message !== 'string') {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            if (Buffer.byteLength(data.message, "utf8") > MAX_SESSION_MESSAGE_CIPHERTEXT_BYTES) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }

            const parsedLocalId = parseSessionMessageLocalId(data.localId);
            if (!parsedLocalId.valid) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            const localId = parsedLocalId.localId;

            const { sid, message } = data;
            if (connection.connectionType === "session-scoped"
                && sid !== connection.sessionId) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                rejectRuntimeConnection(connection, "message targeted another session");
                return;
            }
            log(
                { module: 'websocket' },
                `Received message from socket ${socket.id}: sessionId=${sid}, messageLength=${message.length} bytes, hasLocalId=${localId !== null}, connectionType=${connection.connectionType}, connectionSessionId=${connection.connectionType === 'session-scoped' ? connection.sessionId : 'N/A'}`
            );

            try {
                const result = await persistSessionMessage({
                    userId,
                    sessionId: sid,
                    ciphertext: message,
                    localId,
                    originSocketId: socket.id,
                    ...(connection.connectionType !== "session-scoped"
                        ? {}
                        : connection.replayOnly
                            ? {
                                runtimeAuthorization: {
                                    type: "replay" as const,
                                    ...(connection.sessionInstanceId
                                        ? { sessionInstanceId: connection.sessionInstanceId }
                                        : {}),
                                    ...(connection.runtimeConnectionLeaseId
                                        ? { leaseId: connection.runtimeConnectionLeaseId }
                                        : {}),
                                },
                            }
                            : connection.sessionInstanceId && connection.runtimeConnectionLeaseId
                                ? {
                                    runtimeAuthorization: {
                                        type: "lease" as const,
                                        sessionInstanceId: connection.sessionInstanceId,
                                        leaseId: connection.runtimeConnectionLeaseId,
                                    },
                                }
                                : { runtimeAuthorization: { type: "legacy" as const } }),
                });

                if (result.result === 'success') {
                    // A duplicate producer retry must never reopen an already
                    // delivered notification. Waking is harmless for a
                    // duplicate and immediately drains any new pending row.
                    notificationDispatcher.wake();
                }

                // persistSessionMessage returns only after its database transaction commits.
                callback?.(toSessionMessageAck(result));
                if (result.result === "error"
                    && result.code === "runtime_connection_stale"
                    && connection.connectionType === "session-scoped") {
                    rejectRuntimeConnection(connection, "message transaction lease fence failed");
                }
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in message handler: ${error}`);
                callback?.({ result: 'error', code: 'internal_error', retryable: true });
            }
        });
    });

    socket.on('session-end', async (data: {
        sid?: unknown;
        time?: unknown;
        localId?: unknown;
        sessionInstanceId?: unknown;
    }, callback?: (response: SessionEndAck) => void) => {
        try {
            if (!data
                || typeof data.sid !== 'string'
                || data.sid.length === 0
                || typeof data.time !== 'number'
                || !Number.isFinite(data.time)) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            if (connection.connectionType !== 'session-scoped'
                || data.sid !== connection.sessionId) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            const parsedLocalId = parseSessionMessageLocalId(data.localId);
            const parsedSessionInstanceId = parseSessionMessageLocalId(data.sessionInstanceId);
            if (!parsedLocalId.valid
                || !parsedSessionInstanceId.valid
                || (parsedLocalId.localId === null) !== (parsedSessionInstanceId.localId === null)) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            const localId = parsedLocalId.localId;
            const requestedSessionInstanceId = parsedSessionInstanceId.localId;
            if (connection.replayOnly
                ? requestedSessionInstanceId !== (connection.replayRequestedSessionInstanceId ?? null)
                : requestedSessionInstanceId !== (connection.sessionInstanceId ?? null)) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            const sessionInstanceId = connection.replayOnly
                ? requestedSessionInstanceId ?? connection.sessionInstanceId ?? null
                : requestedSessionInstanceId;
            if (connection.replayOnly && sessionInstanceId === null) {
                // A no-ID replay transport that could not infer/claim an
                // incarnation is duplicate-only. Its terminal marker is an
                // idempotent no-op, never a legacy timestamp-fenced end.
                callback?.({ result: 'success', localId });
                return;
            }
            const { sid, time } = data;
            let t = time;
            if (t > Date.now()) {
                t = Date.now();
            }
            // Resolve session
            const session = await db.session.findUnique({
                where: { id: sid, accountId: userId }
            });
            if (!session) {
                callback?.({ result: 'error', code: 'session_not_found', retryable: false });
                return;
            }

            // Durable producers are fenced by a server-persisted runtime
            // incarnation. Legacy producers retain the timestamp fence.
            const modernEndedAt = sessionInstanceId
                ? await endRuntimeConnectionLease({
                    accountId: userId,
                    sessionId: sid,
                    sessionInstanceId,
                })
                : null;
            const legacyEnded = sessionInstanceId
                ? null
                : await db.session.updateMany({
                    where: {
                        id: sid,
                        accountId: userId,
                        lastActiveAt: { lte: new Date(t) },
                        activeInstanceId: null,
                        // Once a row has entered the lease protocol, only an
                        // incarnation-fenced modern end may terminate it.
                        runtimeConnectionLeaseId: null,
                        runtimeConnectionLeaseInstanceId: null,
                        runtimeConnectionLeaseExpiresAt: null,
                    },
                    data: {
                        lastActiveAt: new Date(t),
                        active: false,
                        // A tombstone prevents a lease-aware reader from ever
                        // falling back after this explicit terminal marker.
                        runtimeConnectionLeaseId: newRuntimeConnectionLeaseTombstone(),
                        runtimeConnectionLeaseInstanceId: null,
                        runtimeConnectionLeaseExpiresAt: new Date(t),
                    },
                });
            const endedAt = modernEndedAt?.getTime() ?? t;
            const didEnd = modernEndedAt !== null || (legacyEnded?.count ?? 0) > 0;

            if (didEnd) {
                // Emit session activity update only when this marker actually
                // won the timestamp fence. Stale retries are successful no-ops.
                sessionActivityPublisher.publish({
                    userId,
                    sessionId: sid,
                    active: false,
                    activeAt: endedAt,
                    thinking: false,
                });
            }
            callback?.({ result: 'success', localId });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in session-end: ${error}`);
            callback?.({ result: 'error', code: 'internal_error', retryable: true });
        }
    });

}
