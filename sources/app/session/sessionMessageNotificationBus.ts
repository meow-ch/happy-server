import type { Redis } from "ioredis";
import type { DurableSessionMessageNotification } from "@/app/events/eventRouter";
import { log } from "@/utils/log";

type RedisPublisher = Pick<Redis, "publish">;
type RedisSubscriber = Pick<Redis, "subscribe" | "unsubscribe" | "on" | "removeListener">;

function isNotification(value: unknown): value is DurableSessionMessageNotification {
    if (!value || typeof value !== "object") return false;
    const candidate = value as Record<string, unknown>;
    if (typeof candidate.userId !== "string") return false;
    if (candidate.originSocketId !== null && typeof candidate.originSocketId !== "string") return false;
    if (!candidate.payload || typeof candidate.payload !== "object") return false;
    const payload = candidate.payload as Record<string, unknown>;
    return typeof payload.id === "string" && typeof payload.seq === "number" && !!payload.body;
}

/**
 * Lossy cross-pod fanout for committed session-message notifications.
 * PostgreSQL is canonical: Pub/Sub is only a low-latency wakeup, and clients
 * repair missed publications with the sequence cursor API.
 */
export class SessionMessageNotificationBus {
    private running = false;

    constructor(
        private readonly publisher: RedisPublisher,
        private readonly subscriber: RedisSubscriber,
        private readonly channel: string,
        private readonly onNotification: (notification: DurableSessionMessageNotification) => Promise<void> | void,
        private readonly publishTimeoutMs = 5_000,
    ) {}

    async start(): Promise<void> {
        if (this.running) return;
        this.running = true;
        this.subscriber.on("message", this.handleMessage);
        try {
            await this.subscriber.subscribe(this.channel);
        } catch (error) {
            this.running = false;
            this.subscriber.removeListener("message", this.handleMessage);
            throw error;
        }
    }

    async publish(notification: DurableSessionMessageNotification): Promise<void> {
        let timeout: NodeJS.Timeout | undefined;
        await Promise.race([
            this.publisher.publish(this.channel, JSON.stringify(notification)),
            new Promise<never>((_, reject) => {
                timeout = setTimeout(
                    () => reject(new Error("Redis session-message notification publish timed out")),
                    this.publishTimeoutMs,
                );
                timeout.unref?.();
            }),
        ]).finally(() => {
            if (timeout) clearTimeout(timeout);
        });
        // PUBLISH returning 0 is valid: there may be no live pod/client to wake.
        // The database row and cursor API remain the recovery source of truth.
    }

    async stop(): Promise<void> {
        if (!this.running) return;
        this.running = false;
        try {
            await this.subscriber.unsubscribe(this.channel);
        } finally {
            this.subscriber.removeListener("message", this.handleMessage);
        }
    }

    private readonly handleMessage = (channel: string, encoded: string): void => {
        if (!this.running || channel !== this.channel) return;
        try {
            const value = JSON.parse(encoded) as unknown;
            if (!isNotification(value)) {
                log({ module: "session-message-notification", level: "error" }, "Skipping invalid Redis Pub/Sub notification");
                return;
            }
            Promise.resolve(this.onNotification(value)).catch((error) => {
                log({ module: "session-message-notification", level: "error" }, `Failed to deliver Redis Pub/Sub notification locally: ${error}`);
            });
        } catch {
            log({ module: "session-message-notification", level: "error" }, "Skipping malformed Redis Pub/Sub notification");
        }
    };
}
