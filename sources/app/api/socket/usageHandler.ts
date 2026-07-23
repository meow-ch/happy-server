import { Socket } from "socket.io";
import { AsyncLock } from "@/utils/lock";
import { db } from "@/storage/db";
import { buildUsageEphemeral, eventRouter } from "@/app/events/eventRouter";
import type { ClientConnection } from "@/app/events/eventRouter";
import { log } from "@/utils/log";
import { runWithRuntimeConnectionOwnerLock } from "@/app/presence/runtimeConnectionLease";
import { rejectRuntimeConnection } from "@/app/api/socket/runtimeConnectionGuard";

export function usageHandler(userId: string, socket: Socket, connection: ClientConnection) {
    const receiveUsageLock = new AsyncLock();
    socket.on('usage-report', async (data: any, callback?: (response: any) => void) => {
        await receiveUsageLock.inLock(async () => {
            try {
                const { key, sessionId, tokens, cost } = data;

                // Validate required fields
                if (!key || typeof key !== 'string') {
                    if (callback) {
                        callback({ success: false, error: 'Invalid key' });
                    }
                    return;
                }

                // Validate tokens and cost objects
                if (!tokens || typeof tokens !== 'object' || typeof tokens.total !== 'number') {
                    if (callback) {
                        callback({ success: false, error: 'Invalid tokens object - must include total' });
                    }
                    return;
                }

                if (!cost || typeof cost !== 'object' || typeof cost.total !== 'number') {
                    if (callback) {
                        callback({ success: false, error: 'Invalid cost object - must include total' });
                    }
                    return;
                }

                const hasSessionId = sessionId !== undefined && sessionId !== null;

                // Validate sessionId if provided
                if (hasSessionId && (typeof sessionId !== 'string' || sessionId.length === 0)) {
                    if (callback) {
                        callback({ success: false, error: 'Invalid sessionId' });
                    }
                    return;
                }

                // A runtime is authorized only for the session bound during its
                // authenticated handshake. Payload data must never broaden that
                // scope to another session owned by the same account.
                if (connection.connectionType === 'session-scoped'
                    && (!hasSessionId || sessionId !== connection.sessionId)) {
                    rejectRuntimeConnection(connection, 'usage report targeted a different session');
                    callback?.({ success: false, error: 'Invalid sessionId' });
                    return;
                }

                try {
                    // Non-runtime clients may report session usage after proving
                    // account ownership. Runtime clients are instead fenced by
                    // their exact incarnation and socket-generation lease below.
                    if (hasSessionId && connection.connectionType !== 'session-scoped') {
                        const session = await db.session.findFirst({
                            where: {
                                id: sessionId,
                                accountId: userId
                            }
                        });

                        if (!session) {
                            if (callback) {
                                callback({ success: false, error: 'Session not found' });
                            }
                            return;
                        }
                    }

                    // Prepare usage data
                    const usageData: PrismaJson.UsageReportData = {
                        tokens,
                        cost
                    };

                    let report: Awaited<ReturnType<typeof db.usageReport.upsert>> | undefined;
                    const save = async (client: Pick<typeof db, 'usageReport'>) => {
                        report = await client.usageReport.upsert({
                            where: {
                                accountId_sessionId_key: {
                                    accountId: userId,
                                    sessionId: hasSessionId ? sessionId : null,
                                    key
                                }
                            },
                            update: {
                                data: usageData,
                                updatedAt: new Date()
                            },
                            create: {
                                accountId: userId,
                                sessionId: hasSessionId ? sessionId : null,
                                key,
                                data: usageData
                            }
                        });

                        log({ module: 'websocket' }, `Usage report saved: key=${key}, sessionId=${hasSessionId ? sessionId : 'none'}, userId=${userId}`);
                    };

                    if (connection.connectionType === 'session-scoped') {
                        const ownerResult = await runWithRuntimeConnectionOwnerLock({
                            accountId: userId,
                            sessionId: connection.sessionId,
                            sessionInstanceId: connection.sessionInstanceId,
                            leaseId: connection.runtimeConnectionLeaseId,
                        }, save);
                        if (ownerResult === 'busy') {
                            callback?.({
                                success: false,
                                outcome: 'not_started',
                                retryable: true,
                                error: 'Runtime connection is busy'
                            });
                            return;
                        }
                        if (ownerResult === 'not_owner') {
                            rejectRuntimeConnection(connection, 'usage report lease lost');
                            callback?.({ success: false, error: 'Runtime connection is stale' });
                            return;
                        }
                    } else {
                        await save(db);
                    }

                    if (!report) throw new Error('Usage report write did not complete');

                    // Publish only after the database operation (and, for a
                    // runtime, its owner-lock transaction) has committed.
                    if (hasSessionId) {
                        const usageEvent = buildUsageEphemeral(sessionId, key, usageData.tokens, usageData.cost);
                        eventRouter.emitEphemeral({
                            userId,
                            payload: usageEvent,
                            recipientFilter: { type: 'user-scoped-only' }
                        });
                    }

                    if (callback) {
                        callback({
                            success: true,
                            reportId: report.id,
                            createdAt: report.createdAt.getTime(),
                            updatedAt: report.updatedAt.getTime()
                        });
                    }
                } catch (error) {
                    log({ module: 'websocket', level: 'error' }, `Failed to save usage report: ${error}`);
                    if (callback) {
                        callback({ success: false, error: 'Failed to save usage report' });
                    }
                }
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in usage-report handler: ${error}`);
                if (callback) {
                    callback({ success: false, error: 'Internal error' });
                }
            }
        });
    });
}
