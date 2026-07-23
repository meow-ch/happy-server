import { randomUUID } from "node:crypto";
import { Prisma } from "@prisma/client";
import { db } from "@/storage/db";
import { buildNewMessageUpdate, DurableSessionMessageNotification } from "@/app/events/eventRouter";
import { log } from "@/utils/log";

export interface ClaimedSessionMessageNotification {
    id: string;
    accountId: string;
    sessionId: string;
    messageId: string;
    updateSeq: number;
    originSocketId: string | null;
    targetRuntimeConnectionLeaseId: string | null;
    targetLegacyRuntimeConnection: boolean;
    attempts: number;
    claimToken: string;
    message: {
        id: string;
        seq: number;
        content: unknown;
        localId: string | null;
        createdAt: Date;
        updatedAt: Date;
    };
}

export interface SessionMessageNotificationRepository {
    claimBatch(now: Date, leaseMs: number, limit: number): Promise<ClaimedSessionMessageNotification[]>;
    markDelivered(notification: ClaimedSessionMessageNotification, now: Date): Promise<void>;
    markFailed(notification: ClaimedSessionMessageNotification, error: string, nextAttemptAt: Date): Promise<void>;
    cleanupDelivered(before: Date): Promise<number>;
}

function pendingClaimWhere(now: Date, leaseCutoff: Date): Prisma.SessionMessageNotificationOutboxWhereInput {
    return {
        deliveredAt: null,
        nextAttemptAt: { lte: now },
        OR: [
            { claimedAt: null },
            { claimedAt: { lt: leaseCutoff } }
        ]
    };
}

export const sessionMessageNotificationRepository: SessionMessageNotificationRepository = {
    async claimBatch(now, leaseMs, limit) {
        const leaseCutoff = new Date(now.getTime() - leaseMs);
        // Only the earliest undelivered message in each session may be claimed.
        // The CAS below decides which pod owns that head; later rows stay
        // blocked through retries and leases, preserving per-session order.
        // Message/outbox creation is serialized by the session row lock and
        // duplicate retries never backfill, so an older row cannot appear
        // between this selection and the lease CAS.
        const candidates = await db.$queryRaw<Array<{ id: string }>>(Prisma.sql`
            SELECT candidate."id"
            FROM "SessionMessageNotificationOutbox" AS candidate
            INNER JOIN "SessionMessage" AS candidate_message
                ON candidate_message."id" = candidate."messageId"
            WHERE candidate."deliveredAt" IS NULL
              AND candidate."nextAttemptAt" <= ${now}
              AND (
                    candidate."claimedAt" IS NULL
                    OR candidate."claimedAt" < ${leaseCutoff}
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM "SessionMessageNotificationOutbox" AS earlier
                    INNER JOIN "SessionMessage" AS earlier_message
                        ON earlier_message."id" = earlier."messageId"
                    WHERE earlier."sessionId" = candidate."sessionId"
                      AND earlier."deliveredAt" IS NULL
                      AND (
                            earlier_message."seq" < candidate_message."seq"
                            OR (
                                earlier_message."seq" = candidate_message."seq"
                                AND earlier."id" < candidate."id"
                            )
                      )
              )
            ORDER BY candidate."nextAttemptAt" ASC, candidate."createdAt" ASC, candidate."id" ASC
            LIMIT ${limit}
        `);

        const claimed: ClaimedSessionMessageNotification[] = [];
        for (const candidate of candidates) {
            const claimToken = randomUUID();
            const { count } = await db.sessionMessageNotificationOutbox.updateMany({
                where: {
                    id: candidate.id,
                    ...pendingClaimWhere(now, leaseCutoff)
                },
                data: {
                    claimedAt: now,
                    claimToken,
                    attempts: { increment: 1 }
                }
            });
            if (count !== 1) continue;

            const notification = await db.sessionMessageNotificationOutbox.findUnique({
                where: { id: candidate.id },
                select: {
                    id: true,
                    accountId: true,
                    sessionId: true,
                    messageId: true,
                    updateSeq: true,
                    originSocketId: true,
                    targetRuntimeConnectionLeaseId: true,
                    targetLegacyRuntimeConnection: true,
                    attempts: true,
                    claimToken: true,
                    message: {
                        select: {
                            id: true,
                            seq: true,
                            content: true,
                            localId: true,
                            createdAt: true,
                            updatedAt: true
                        }
                    }
                }
            });
            if (notification?.claimToken !== claimToken) continue;
            claimed.push(notification as ClaimedSessionMessageNotification);
        }
        return claimed;
    },

    async markDelivered(notification, now) {
        await db.sessionMessageNotificationOutbox.updateMany({
            where: {
                id: notification.id,
                claimToken: notification.claimToken,
                deliveredAt: null
            },
            data: {
                deliveredAt: now,
                claimedAt: null,
                claimToken: null,
                lastError: null
            }
        });
    },

    async markFailed(notification, error, nextAttemptAt) {
        await db.sessionMessageNotificationOutbox.updateMany({
            where: {
                id: notification.id,
                claimToken: notification.claimToken,
                deliveredAt: null
            },
            data: {
                nextAttemptAt,
                claimedAt: null,
                claimToken: null,
                lastError: error.slice(0, 1000)
            }
        });
    },

    async cleanupDelivered(before) {
        const { count } = await db.sessionMessageNotificationOutbox.deleteMany({
            where: { deliveredAt: { lt: before } }
        });
        return count;
    }
};

export interface SessionMessageNotificationDispatcherOptions {
    batchSize?: number;
    pollIntervalMs?: number;
    leaseMs?: number;
    retryBaseMs?: number;
    retryMaxMs?: number;
    deliveredRetentionMs?: number;
    cleanupIntervalMs?: number;
    now?: () => Date;
    random?: () => number;
}

export class SessionMessageNotificationDispatcher {
    private readonly batchSize: number;
    private readonly pollIntervalMs: number;
    private readonly leaseMs: number;
    private readonly retryBaseMs: number;
    private readonly retryMaxMs: number;
    private readonly deliveredRetentionMs: number;
    private readonly cleanupIntervalMs: number;
    private readonly now: () => Date;
    private readonly random: () => number;
    private timer: NodeJS.Timeout | null = null;
    private running: Promise<void> | null = null;
    private wakeAgain = false;
    private stopped = true;
    private lastCleanupAt = 0;

    constructor(
        private readonly repository: SessionMessageNotificationRepository,
        private readonly publish: (notification: DurableSessionMessageNotification) => Promise<void>,
        options: SessionMessageNotificationDispatcherOptions = {}
    ) {
        this.batchSize = options.batchSize ?? 25;
        this.pollIntervalMs = options.pollIntervalMs ?? 1000;
        this.leaseMs = options.leaseMs ?? 30_000;
        this.retryBaseMs = options.retryBaseMs ?? 500;
        this.retryMaxMs = options.retryMaxMs ?? 60_000;
        this.deliveredRetentionMs = options.deliveredRetentionMs ?? 24 * 60 * 60 * 1000;
        this.cleanupIntervalMs = options.cleanupIntervalMs ?? 60 * 60 * 1000;
        this.now = options.now ?? (() => new Date());
        this.random = options.random ?? Math.random;
    }

    start(): void {
        if (!this.stopped) return;
        this.stopped = false;
        this.timer = setInterval(() => this.wake(), this.pollIntervalMs);
        this.timer.unref?.();
        this.wake();
    }

    wake(): void {
        if (this.stopped) return;
        if (this.running) {
            this.wakeAgain = true;
            return;
        }
        this.running = this.drain()
            .catch((error) => {
                log({ module: "session-message-outbox", level: "error" }, `Notification dispatcher failed: ${error}`);
            })
            .finally(() => {
                this.running = null;
                if (this.wakeAgain && !this.stopped) {
                    this.wakeAgain = false;
                    this.wake();
                }
            });
    }

    async stop(): Promise<void> {
        this.stopped = true;
        if (this.timer) clearInterval(this.timer);
        this.timer = null;
        await this.running;
    }

    private async drain(): Promise<void> {
        do {
            this.wakeAgain = false;
            const now = this.now();
            if (now.getTime() - this.lastCleanupAt >= this.cleanupIntervalMs) {
                this.lastCleanupAt = now.getTime();
                await this.repository.cleanupDelivered(new Date(now.getTime() - this.deliveredRetentionMs));
            }

            const notifications = await this.repository.claimBatch(now, this.leaseMs, this.batchSize);
            for (const notification of notifications) {
                const payload = buildNewMessageUpdate(
                    notification.message,
                    notification.sessionId,
                    notification.updateSeq,
                    notification.id
                );
                try {
                    await this.publish({
                        userId: notification.accountId,
                        payload,
                        originSocketId: notification.originSocketId,
                        targetRuntimeConnectionLeaseId: notification.targetRuntimeConnectionLeaseId,
                        targetLegacyRuntimeConnection: notification.targetLegacyRuntimeConnection,
                    });
                    await this.repository.markDelivered(notification, this.now());
                } catch (error) {
                    const message = error instanceof Error ? error.message : String(error);
                    const ceiling = Math.min(
                        this.retryMaxMs,
                        this.retryBaseMs * (2 ** Math.min(Math.max(notification.attempts - 1, 0), 16))
                    );
                    const retryDelay = Math.max(1, Math.floor(this.random() * (ceiling + 1)));
                    await this.repository.markFailed(
                        notification,
                        message,
                        new Date(this.now().getTime() + retryDelay)
                    );
                }
            }

            // claimBatch returns at most one row per session. Re-query after a
            // successful batch so the newly unblocked next sequence is sent
            // immediately instead of adding one poll interval per message.
            if (notifications.length > 0) this.wakeAgain = true;
        } while (this.wakeAgain && !this.stopped);
    }
}
