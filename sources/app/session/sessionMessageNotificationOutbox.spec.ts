import { describe, expect, it, vi } from "vitest";

vi.mock("@/storage/files", () => ({
    getPublicUrl: (path: string) => path
}));

import {
    ClaimedSessionMessageNotification,
    SessionMessageNotificationDispatcher,
    SessionMessageNotificationRepository
} from "@/app/session/sessionMessageNotificationOutbox";

function notification(overrides: Partial<ClaimedSessionMessageNotification> = {}): ClaimedSessionMessageNotification {
    return {
        id: "notification-1",
        accountId: "account-1",
        sessionId: "session-1",
        messageId: "message-1",
        updateSeq: 31,
        originSocketId: "socket-1",
        attempts: 1,
        claimToken: "claim-1",
        message: {
            id: "message-1",
            seq: 7,
            content: { t: "encrypted", c: "ciphertext" },
            localId: "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3",
            createdAt: new Date("2026-07-19T10:00:00.000Z"),
            updatedAt: new Date("2026-07-19T10:00:01.000Z")
        },
        ...overrides
    };
}

function repository(claimed: ClaimedSessionMessageNotification[] = []) {
    return {
        claimBatch: vi.fn().mockResolvedValue(claimed),
        markDelivered: vi.fn().mockResolvedValue(undefined),
        markFailed: vi.fn().mockResolvedValue(undefined),
        cleanupDelivered: vi.fn().mockResolvedValue(0)
    } satisfies SessionMessageNotificationRepository;
}

describe("SessionMessageNotificationDispatcher", () => {
    it("publishes the stable outbox id and marks the claimed row delivered", async () => {
        const claimed = notification();
        const store = repository([claimed]);
        const publish = vi.fn().mockResolvedValue(undefined);
        const now = new Date("2026-07-19T10:05:00.000Z");
        const dispatcher = new SessionMessageNotificationDispatcher(store, publish, {
            batchSize: 25,
            pollIntervalMs: 60_000,
            now: () => now
        });

        dispatcher.start();
        await dispatcher.stop();

        expect(publish).toHaveBeenCalledWith(expect.objectContaining({
            userId: "account-1",
            originSocketId: "socket-1",
            payload: expect.objectContaining({
                id: "notification-1",
                seq: 31,
                body: expect.objectContaining({
                    t: "new-message",
                    sid: "session-1"
                })
            })
        }));
        expect(store.markDelivered).toHaveBeenCalledWith(claimed, now);
        expect(store.markFailed).not.toHaveBeenCalled();
    });

    it("releases a failed claim with bounded full-jitter backoff", async () => {
        const claimed = notification({ attempts: 3 });
        const store = repository([claimed]);
        const publish = vi.fn().mockRejectedValue(new Error("redis unavailable"));
        const now = new Date("2026-07-19T10:05:00.000Z");
        const dispatcher = new SessionMessageNotificationDispatcher(store, publish, {
            batchSize: 25,
            pollIntervalMs: 60_000,
            retryBaseMs: 500,
            retryMaxMs: 60_000,
            random: () => 0.5,
            now: () => now
        });

        dispatcher.start();
        await dispatcher.stop();

        expect(store.markDelivered).not.toHaveBeenCalled();
        expect(store.markFailed).toHaveBeenCalledWith(
            claimed,
            "redis unavailable",
            new Date(now.getTime() + 1000)
        );
    });

});
