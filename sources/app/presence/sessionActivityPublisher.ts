import {
    buildSessionActivityEphemeral,
    eventRouter,
} from "@/app/events/eventRouter";
import { sessionActivityPublicationsCounter } from "@/app/monitoring/metrics2";

const DEFAULT_REFRESH_INTERVAL_MS = 30_000;
const DEFAULT_STALE_ENTRY_TTL_MS = 20 * 60_000;

interface SessionActivityState {
    active: boolean;
    thinking: boolean;
    lastPublishedAt: number;
    lastObservedAt: number;
}

export interface SessionActivityPublication {
    userId: string;
    sessionId: string;
    active: boolean;
    activeAt: number;
    thinking: boolean;
}

interface SessionActivityPublisherOptions {
    refreshIntervalMs?: number;
    staleEntryTtlMs?: number;
    now?: () => number;
}

/**
 * Coalesces high-frequency session heartbeats before Socket.IO Redis fanout.
 * State transitions are immediate; unchanged presence is periodically refreshed.
 */
export class SessionActivityPublisher {
    private readonly states = new Map<string, SessionActivityState>();
    private readonly refreshIntervalMs: number;
    private readonly staleEntryTtlMs: number;
    private readonly now: () => number;
    private lastCleanupAt: number;

    constructor(options: SessionActivityPublisherOptions = {}) {
        this.refreshIntervalMs = options.refreshIntervalMs ?? DEFAULT_REFRESH_INTERVAL_MS;
        this.staleEntryTtlMs = options.staleEntryTtlMs ?? DEFAULT_STALE_ENTRY_TTL_MS;
        this.now = options.now ?? Date.now;
        if (!Number.isSafeInteger(this.refreshIntervalMs) || this.refreshIntervalMs <= 0) {
            throw new Error("Session activity refresh interval must be a positive integer");
        }
        if (!Number.isSafeInteger(this.staleEntryTtlMs) || this.staleEntryTtlMs <= this.refreshIntervalMs) {
            throw new Error("Session activity state TTL must exceed its refresh interval");
        }
        this.lastCleanupAt = this.now();
    }

    publish(activity: SessionActivityPublication): boolean {
        const observedAt = this.now();
        this.cleanupIfNeeded(observedAt);

        const key = this.getKey(activity.userId, activity.sessionId);
        const previous = this.states.get(key);
        let reason: "first" | "state_change" | "refresh" | null = null;
        if (!previous) {
            reason = "first";
        } else if (previous.active !== activity.active || previous.thinking !== activity.thinking) {
            reason = "state_change";
        } else if (observedAt - previous.lastPublishedAt >= this.refreshIntervalMs) {
            reason = "refresh";
        }

        if (!reason) {
            previous!.lastObservedAt = observedAt;
            sessionActivityPublicationsCounter.inc({ result: "coalesced", reason: "unchanged" });
            return false;
        }

        eventRouter.emitEphemeral({
            userId: activity.userId,
            payload: buildSessionActivityEphemeral(
                activity.sessionId,
                activity.active,
                activity.activeAt,
                activity.thinking,
            ),
            recipientFilter: { type: "user-scoped-only" },
        });
        this.states.set(key, {
            active: activity.active,
            thinking: activity.thinking,
            lastPublishedAt: observedAt,
            lastObservedAt: observedAt,
        });
        sessionActivityPublicationsCounter.inc({ result: "published", reason });
        return true;
    }

    private getKey(userId: string, sessionId: string): string {
        return `${userId.length}:${userId}${sessionId}`;
    }

    private cleanupIfNeeded(observedAt: number): void {
        if (observedAt - this.lastCleanupAt < this.staleEntryTtlMs) return;
        this.lastCleanupAt = observedAt;
        for (const [key, state] of this.states) {
            if (observedAt - state.lastObservedAt >= this.staleEntryTtlMs) {
                this.states.delete(key);
            }
        }
    }
}

export const sessionActivityPublisher = new SessionActivityPublisher();
