import { db } from "@/storage/db";
import { log } from "@/utils/log";
import {
    activityCacheDatabaseUpdatesCounter,
    activityCacheFlushCounter,
    activityCacheFlushDurationHistogram,
    activityCacheFlushInProgressGauge,
    activityCachePendingUpdatesGauge,
    databaseUpdatesSkippedCounter,
    sessionCacheCounter,
} from "@/app/monitoring/metrics2";

interface SessionPendingUpdate {
    timestamp: number;
    sessionInstanceId?: string;
}

interface SessionCacheEntry {
    validUntil: number;
    lastUpdateSent: number;
    pendingUpdate: SessionPendingUpdate | null;
    inFlightUpdate: SessionPendingUpdate | null;
    userId: string;
}

interface MachineCacheEntry {
    validUntil: number;
    lastUpdateSent: number;
    pendingUpdate: number | null;
    inFlightUpdate: number | null;
    userId: string;
}

interface SessionDatabaseUpdate extends SessionPendingUpdate {
    type: "session";
    id: string;
    entry: SessionCacheEntry;
}

interface MachineDatabaseUpdate {
    type: "machine";
    id: string;
    timestamp: number;
    userId: string;
    entry: MachineCacheEntry;
}

type ActivityDatabaseUpdate = SessionDatabaseUpdate | MachineDatabaseUpdate;

interface ActivityCacheOptions {
    autoStart?: boolean;
    batchIntervalMs?: number;
    maxFlushConcurrency?: number;
}

const CACHE_TTL_MS = 30_000;
const UPDATE_THRESHOLD_MS = 30_000;
const DEFAULT_BATCH_INTERVAL_MS = 5_000;
const DEFAULT_MAX_FLUSH_CONCURRENCY = 8;

export class ActivityCache {
    private readonly sessionCache = new Map<string, SessionCacheEntry>();
    private readonly machineCache = new Map<string, MachineCacheEntry>();
    private readonly batchIntervalMs: number;
    private readonly maxFlushConcurrency: number;
    private batchTimer: NodeJS.Timeout | null = null;
    private flushPromise: Promise<void> | null = null;
    private shutdownPromise: Promise<void> | null = null;
    private flushAgain = false;
    private isShuttingDown = false;

    constructor(options: ActivityCacheOptions = {}) {
        this.batchIntervalMs = options.batchIntervalMs ?? DEFAULT_BATCH_INTERVAL_MS;
        this.maxFlushConcurrency = options.maxFlushConcurrency ?? DEFAULT_MAX_FLUSH_CONCURRENCY;
        if (!Number.isSafeInteger(this.batchIntervalMs) || this.batchIntervalMs <= 0) {
            throw new Error("Activity cache batch interval must be a positive integer");
        }
        if (!Number.isSafeInteger(this.maxFlushConcurrency) || this.maxFlushConcurrency <= 0) {
            throw new Error("Activity cache flush concurrency must be a positive integer");
        }
        if (options.autoStart !== false) {
            this.startBatchTimer();
        }
    }

    private startBatchTimer(): void {
        if (this.batchTimer) {
            clearInterval(this.batchTimer);
        }

        this.batchTimer = setInterval(() => {
            void this.flushPendingUpdates().catch(error => {
                log({ module: "session-cache", level: "error" }, `Unexpected activity flush error: ${error}`);
            });
        }, this.batchIntervalMs);
        this.batchTimer.unref?.();
    }

    async isSessionValid(sessionId: string, userId: string): Promise<boolean> {
        const now = Date.now();
        const cached = this.sessionCache.get(sessionId);

        if (cached && cached.validUntil > now && cached.userId === userId) {
            sessionCacheCounter.inc({ operation: "session_validation", result: "hit" });
            return true;
        }

        sessionCacheCounter.inc({ operation: "session_validation", result: "miss" });
        try {
            const session = await db.session.findUnique({
                where: { id: sessionId, accountId: userId }
            });

            if (!session) return false;
            const current = this.sessionCache.get(sessionId);
            if (current && current.userId === userId) {
                current.validUntil = now + CACHE_TTL_MS;
                current.lastUpdateSent = Math.max(current.lastUpdateSent, session.lastActiveAt.getTime());
            } else {
                this.sessionCache.set(sessionId, {
                    validUntil: now + CACHE_TTL_MS,
                    lastUpdateSent: session.lastActiveAt.getTime(),
                    pendingUpdate: null,
                    inFlightUpdate: null,
                    userId
                });
            }
            return true;
        } catch (error) {
            log({ module: "session-cache", level: "error" }, `Error validating session ${sessionId}: ${error}`);
            return false;
        }
    }

    async isMachineValid(machineId: string, userId: string): Promise<boolean> {
        const now = Date.now();
        const cached = this.machineCache.get(machineId);

        if (cached && cached.validUntil > now && cached.userId === userId) {
            sessionCacheCounter.inc({ operation: "machine_validation", result: "hit" });
            return true;
        }

        sessionCacheCounter.inc({ operation: "machine_validation", result: "miss" });
        try {
            const machine = await db.machine.findUnique({
                where: {
                    accountId_id: {
                        accountId: userId,
                        id: machineId
                    }
                }
            });

            if (!machine) return false;
            const persistedLastActiveAt = machine.lastActiveAt?.getTime() || 0;
            const current = this.machineCache.get(machineId);
            if (current && current.userId === userId) {
                current.validUntil = now + CACHE_TTL_MS;
                current.lastUpdateSent = Math.max(current.lastUpdateSent, persistedLastActiveAt);
            } else {
                this.machineCache.set(machineId, {
                    validUntil: now + CACHE_TTL_MS,
                    lastUpdateSent: persistedLastActiveAt,
                    pendingUpdate: null,
                    inFlightUpdate: null,
                    userId
                });
            }
            return true;
        } catch (error) {
            log({ module: "session-cache", level: "error" }, `Error validating machine ${machineId}: ${error}`);
            return false;
        }
    }

    queueSessionUpdate(sessionId: string, timestamp: number, sessionInstanceId?: string): boolean {
        const cached = this.sessionCache.get(sessionId);
        if (!cached || this.isShuttingDown) return false;

        const inFlightUpdate = cached.inFlightUpdate;
        const pendingUpdate = cached.pendingUpdate;
        const matchingInFlightTimestamp = inFlightUpdate
            && inFlightUpdate.sessionInstanceId === sessionInstanceId
            ? inFlightUpdate.timestamp
            : Number.NEGATIVE_INFINITY;
        const matchingPendingTimestamp = pendingUpdate
            && pendingUpdate.sessionInstanceId === sessionInstanceId
            ? pendingUpdate.timestamp
            : Number.NEGATIVE_INFINITY;
        const activityWatermark = Math.max(
            cached.lastUpdateSent,
            matchingInFlightTimestamp,
            matchingPendingTimestamp,
        );
        const timeDiff = Math.abs(timestamp - activityWatermark);
        if (timeDiff > UPDATE_THRESHOLD_MS) {
            if (!cached.pendingUpdate || timestamp >= cached.pendingUpdate.timestamp) {
                cached.pendingUpdate = { timestamp, sessionInstanceId };
                return true;
            }
        }

        databaseUpdatesSkippedCounter.inc({ type: "session" });
        return false;
    }

    queueMachineUpdate(machineId: string, timestamp: number): boolean {
        const cached = this.machineCache.get(machineId);
        if (!cached || this.isShuttingDown) return false;

        const activityWatermark = Math.max(
            cached.lastUpdateSent,
            cached.inFlightUpdate ?? Number.NEGATIVE_INFINITY,
            cached.pendingUpdate ?? Number.NEGATIVE_INFINITY,
        );
        const timeDiff = Math.abs(timestamp - activityWatermark);
        if (timeDiff > UPDATE_THRESHOLD_MS) {
            if (cached.pendingUpdate === null || timestamp >= cached.pendingUpdate) {
                cached.pendingUpdate = timestamp;
                return true;
            }
        }

        databaseUpdatesSkippedCounter.inc({ type: "machine" });
        return false;
    }

    /** Runs one failure-safe flush at a time; overlapping callers join it. */
    flushPendingUpdates(): Promise<void> {
        if (this.flushPromise) {
            this.flushAgain = true;
            activityCacheFlushCounter.inc({ result: "coalesced" });
            return this.flushPromise;
        }

        const startedAt = Date.now();
        activityCacheFlushCounter.inc({ result: "started" });
        activityCacheFlushInProgressGauge.set(1);
        const flushPromise = this.runFlushLoop().finally(() => {
            this.flushPromise = null;
            activityCacheFlushDurationHistogram.observe((Date.now() - startedAt) / 1_000);
            activityCacheFlushInProgressGauge.set(0);
        });
        this.flushPromise = flushPromise;
        return flushPromise;
    }

    private async runFlushLoop(): Promise<void> {
        let failureCount = 0;
        do {
            this.flushAgain = false;
            failureCount = await this.flushBatch();
        } while (this.flushAgain && failureCount === 0);
        activityCacheFlushCounter.inc({ result: failureCount === 0 ? "completed" : "partial_failure" });
    }

    private async flushBatch(): Promise<number> {
        const updates = this.takePendingUpdates();
        const sessionCount = updates.filter(update => update.type === "session").length;
        const machineCount = updates.length - sessionCount;
        activityCachePendingUpdatesGauge.set({ type: "session" }, sessionCount);
        activityCachePendingUpdatesGauge.set({ type: "machine" }, machineCount);

        const failures = await this.runBounded(updates, update => this.persistUpdate(update));
        const failedUpdates = failures.filter((error): error is Error => error !== null);
        const pendingCounts = this.getPendingCounts();
        activityCachePendingUpdatesGauge.set({ type: "session" }, pendingCounts.session);
        activityCachePendingUpdatesGauge.set({ type: "machine" }, pendingCounts.machine);

        if (sessionCount > 0) {
            log({ module: "session-cache" }, `Processed ${sessionCount} session updates`);
        }
        if (machineCount > 0) {
            log({ module: "session-cache" }, `Processed ${machineCount} machine updates`);
        }
        if (failedUpdates.length > 0) {
            log(
                { module: "session-cache", level: "error" },
                `Failed ${failedUpdates.length} of ${updates.length} activity updates; queued for retry: ${failedUpdates[0]}`,
            );
        }
        return failedUpdates.length;
    }

    private takePendingUpdates(): ActivityDatabaseUpdate[] {
        const updates: ActivityDatabaseUpdate[] = [];
        for (const [sessionId, entry] of this.sessionCache) {
            if (!entry.pendingUpdate || entry.inFlightUpdate) continue;
            const pendingUpdate = entry.pendingUpdate;
            entry.pendingUpdate = null;
            entry.inFlightUpdate = pendingUpdate;
            updates.push({
                type: "session",
                id: sessionId,
                entry,
                ...pendingUpdate,
            });
        }
        for (const [machineId, entry] of this.machineCache) {
            if (entry.pendingUpdate === null || entry.inFlightUpdate !== null) continue;
            const timestamp = entry.pendingUpdate;
            entry.pendingUpdate = null;
            entry.inFlightUpdate = timestamp;
            updates.push({
                type: "machine",
                id: machineId,
                timestamp,
                userId: entry.userId,
                entry,
            });
        }
        return updates;
    }

    private async persistUpdate(update: ActivityDatabaseUpdate): Promise<Error | null> {
        try {
            if (update.type === "session") {
                await db.session.updateMany({
                    where: {
                        id: update.id,
                        lastActiveAt: { lt: new Date(update.timestamp) },
                        ...(update.sessionInstanceId
                            ? {
                                activeInstanceId: update.sessionInstanceId,
                                active: true,
                            }
                            : {}),
                    },
                    data: { lastActiveAt: new Date(update.timestamp), active: true }
                });
                if (this.sessionCache.get(update.id) === update.entry) {
                    update.entry.lastUpdateSent = Math.max(update.entry.lastUpdateSent, update.timestamp);
                    update.entry.inFlightUpdate = null;
                    if (update.entry.pendingUpdate
                        && update.entry.pendingUpdate.sessionInstanceId === update.sessionInstanceId
                        && Math.abs(update.entry.pendingUpdate.timestamp - update.entry.lastUpdateSent) <= UPDATE_THRESHOLD_MS) {
                        update.entry.pendingUpdate = null;
                    }
                }
            } else {
                await db.machine.update({
                    where: {
                        accountId_id: {
                            accountId: update.userId,
                            id: update.id
                        }
                    },
                    data: { lastActiveAt: new Date(update.timestamp) }
                });
                if (this.machineCache.get(update.id) === update.entry) {
                    update.entry.lastUpdateSent = Math.max(update.entry.lastUpdateSent, update.timestamp);
                    update.entry.inFlightUpdate = null;
                    if (update.entry.pendingUpdate !== null
                        && Math.abs(update.entry.pendingUpdate - update.entry.lastUpdateSent) <= UPDATE_THRESHOLD_MS) {
                        update.entry.pendingUpdate = null;
                    }
                }
            }
            activityCacheDatabaseUpdatesCounter.inc({ type: update.type, result: "success" });
            return null;
        } catch (error) {
            if (isMissingRecordError(error)) {
                this.discardMissingUpdate(update);
                activityCacheDatabaseUpdatesCounter.inc({ type: update.type, result: "not_found" });
                return null;
            }
            this.requeueFailedUpdate(update);
            activityCacheDatabaseUpdatesCounter.inc({ type: update.type, result: "failure" });
            return error instanceof Error ? error : new Error(String(error));
        }
    }

    private requeueFailedUpdate(update: ActivityDatabaseUpdate): void {
        if (update.type === "session") {
            if (this.sessionCache.get(update.id) !== update.entry) return;
            update.entry.inFlightUpdate = null;
            if (!update.entry.pendingUpdate) {
                update.entry.pendingUpdate = {
                    timestamp: update.timestamp,
                    sessionInstanceId: update.sessionInstanceId,
                };
            }
            return;
        }

        if (this.machineCache.get(update.id) !== update.entry) return;
        update.entry.inFlightUpdate = null;
        if (update.entry.pendingUpdate === null) {
            update.entry.pendingUpdate = update.timestamp;
        }
    }

    private discardMissingUpdate(update: ActivityDatabaseUpdate): void {
        if (update.type === "session") {
            if (this.sessionCache.get(update.id) === update.entry) {
                this.sessionCache.delete(update.id);
            }
            return;
        }
        if (this.machineCache.get(update.id) === update.entry) {
            this.machineCache.delete(update.id);
        }
    }

    private async runBounded<T, R>(items: T[], worker: (item: T) => Promise<R>): Promise<R[]> {
        const results = new Array<R>(items.length);
        let nextIndex = 0;
        const workers = Array.from(
            { length: Math.min(this.maxFlushConcurrency, items.length) },
            async () => {
                while (nextIndex < items.length) {
                    const index = nextIndex;
                    nextIndex += 1;
                    results[index] = await worker(items[index]);
                }
            },
        );
        await Promise.all(workers);
        return results;
    }

    private getPendingCounts(): { session: number; machine: number } {
        let session = 0;
        let machine = 0;
        for (const entry of this.sessionCache.values()) {
            if (entry.pendingUpdate) session += 1;
        }
        for (const entry of this.machineCache.values()) {
            if (entry.pendingUpdate !== null) machine += 1;
        }
        return { session, machine };
    }

    cleanup(): void {
        const now = Date.now();
        for (const [sessionId, entry] of this.sessionCache) {
            if (entry.validUntil < now && !entry.pendingUpdate && !entry.inFlightUpdate) {
                this.sessionCache.delete(sessionId);
            }
        }
        for (const [machineId, entry] of this.machineCache) {
            if (entry.validUntil < now && entry.pendingUpdate === null && entry.inFlightUpdate === null) {
                this.machineCache.delete(machineId);
            }
        }
    }

    shutdown(): Promise<void> {
        if (this.shutdownPromise) return this.shutdownPromise;
        this.isShuttingDown = true;
        if (this.batchTimer) {
            clearInterval(this.batchTimer);
            this.batchTimer = null;
        }
        this.shutdownPromise = this.flushPendingUpdates().catch(error => {
            log({ module: "session-cache", level: "error" }, `Unexpected final activity flush error: ${error}`);
        });
        return this.shutdownPromise;
    }
}

function isMissingRecordError(error: unknown): boolean {
    return typeof error === "object"
        && error !== null
        && "code" in error
        && error.code === "P2025";
}

export const activityCache = new ActivityCache();

const cleanupTimer = setInterval(() => {
    activityCache.cleanup();
}, 5 * 60_000);
cleanupTimer.unref?.();
