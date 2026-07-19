import { sessionAliveEventsCounter, websocketEventsCounter } from "@/app/monitoring/metrics2";
import { activityCache } from "@/app/presence/sessionCache";
import { buildSessionActivityEphemeral, buildUpdateSessionUpdate, ClientConnection, eventRouter } from "@/app/events/eventRouter";
import { db } from "@/storage/db";
import { allocateUserSeq } from "@/storage/seq";
import { AsyncLock } from "@/utils/lock";
import { log } from "@/utils/log";
import { randomKeyNaked } from "@/utils/randomKeyNaked";
import { Socket } from "socket.io";
import { persistSessionMessage, SessionMessageAck, toSessionMessageAck } from "@/app/session/sessionMessageCreate";
import { SessionMessageNotificationDispatcher } from "@/app/session/sessionMessageNotificationOutbox";
import { parseSessionMessageLocalId } from "@/app/api/socket/sessionMessageValidation";

type SessionEndAck =
    | { result: "success"; localId: string | null }
    | { result: "error"; code: "invalid_request" | "session_not_found" | "internal_error"; retryable: boolean };

export const MAX_SESSION_MESSAGE_CIPHERTEXT_BYTES = 7 * 1024 * 1024;

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
            const { count } = await db.session.updateMany({
                where: { id: sid, metadataVersion: expectedVersion },
                data: {
                    metadata: metadata,
                    metadataVersion: expectedVersion + 1
                }
            });
            if (count === 0) {
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
            const { count } = await db.session.updateMany({
                where: { id: sid, agentStateVersion: expectedVersion },
                data: {
                    agentState: agentState,
                    agentStateVersion: expectedVersion + 1
                }
            });
            if (count === 0) {
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

            let t = data.time;
            if (t > Date.now()) {
                t = Date.now();
            }
            if (t < Date.now() - 1000 * 60 * 10) {
                return;
            }

            const { sid, thinking } = data;

            // Check session validity using cache
            const isValid = await activityCache.isSessionValid(sid, userId);
            if (!isValid) {
                return;
            }

            // Queue database update (will only update if time difference is significant)
            activityCache.queueSessionUpdate(
                sid,
                t,
                connection.connectionType === 'session-scoped'
                    ? connection.sessionInstanceId
                    : undefined
            );

            // Emit session activity update
            const sessionActivity = buildSessionActivityEphemeral(sid, true, t, thinking || false);
            eventRouter.emitEphemeral({
                userId,
                payload: sessionActivity,
                recipientFilter: { type: 'user-scoped-only' }
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
                    originSocketId: socket.id
                });

                if (result.result === 'success') {
                    // A duplicate producer retry must never reopen an already
                    // delivered notification. Waking is harmless for a
                    // duplicate and immediately drains any new pending row.
                    notificationDispatcher.wake();
                }

                // persistSessionMessage returns only after its database transaction commits.
                callback?.(toSessionMessageAck(result));
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
            const parsedLocalId = parseSessionMessageLocalId(data.localId);
            const parsedSessionInstanceId = parseSessionMessageLocalId(data.sessionInstanceId);
            if (!parsedLocalId.valid
                || !parsedSessionInstanceId.valid
                || (parsedLocalId.localId === null) !== (parsedSessionInstanceId.localId === null)) {
                callback?.({ result: 'error', code: 'invalid_request', retryable: false });
                return;
            }
            const localId = parsedLocalId.localId;
            const sessionInstanceId = parsedSessionInstanceId.localId;
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

            const endedAt = sessionInstanceId ? Date.now() : t;
            // Durable producers are fenced by a server-persisted runtime
            // incarnation. Legacy producers retain the timestamp fence.
            const ended = await db.session.updateMany({
                where: {
                    id: sid,
                    accountId: userId,
                    ...(sessionInstanceId
                        ? {
                            activeInstanceId: sessionInstanceId,
                            active: true,
                        }
                        : { lastActiveAt: { lte: new Date(t) } }),
                },
                data: {
                    lastActiveAt: new Date(endedAt),
                    active: false,
                }
            });

            if (ended.count > 0) {
                // Emit session activity update only when this marker actually
                // won the timestamp fence. Stale retries are successful no-ops.
                const sessionActivity = buildSessionActivityEphemeral(sid, false, endedAt, false);
                eventRouter.emitEphemeral({
                    userId,
                    payload: sessionActivity,
                    recipientFilter: { type: 'user-scoped-only' }
                });
            }
            callback?.({ result: 'success', localId });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in session-end: ${error}`);
            callback?.({ result: 'error', code: 'internal_error', retryable: true });
        }
    });

}
