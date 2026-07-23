import { describe, expect, it, vi } from "vitest";
import { SessionMessageNotificationBus } from "@/app/session/sessionMessageNotificationBus";
import { listSessionMessagesAfterSeq } from "@/app/session/sessionMessageList";

describe("session message Redis-loss recovery contract", () => {
    it("replays committed PostgreSQL rows by sequence after Pub/Sub had no subscriber", async () => {
        const publish = vi.fn().mockResolvedValue(0);
        const bus = new SessionMessageNotificationBus(
            { publish } as never,
            {
                subscribe: vi.fn(),
                unsubscribe: vi.fn(),
                on: vi.fn(),
                removeListener: vi.fn(),
            } as never,
            "happy:test-notifications",
            vi.fn(),
        );
        await bus.publish({
            userId: "account-1",
            originSocketId: null,
            targetRuntimeConnectionLeaseId: "lease-1",
            targetLegacyRuntimeConnection: false,
            payload: {
                id: "notification-8",
                seq: 40,
                body: { t: "new-message", sid: "session-1" },
                createdAt: 1,
            },
        });

        const findMany = vi.fn().mockResolvedValue([
            {
                id: "message-8",
                seq: 8,
                localId: null,
                content: { t: "encrypted", c: "ciphertext-8" },
                createdAt: new Date("2026-07-19T10:00:08.000Z"),
                updatedAt: new Date("2026-07-19T10:00:08.000Z"),
            },
            {
                id: "message-9",
                seq: 9,
                localId: null,
                content: { t: "encrypted", c: "ciphertext-9" },
                createdAt: new Date("2026-07-19T10:00:09.000Z"),
                updatedAt: new Date("2026-07-19T10:00:09.000Z"),
            },
        ]);

        const replay = await listSessionMessagesAfterSeq(
            { sessionMessage: { findMany } } as never,
            { sessionId: "session-1", afterSeq: 7, limit: 500 },
        );

        expect(publish).toHaveBeenCalledTimes(1);
        expect(replay.messages.map((message) => message.seq)).toEqual([8, 9]);
        expect(replay.nextAfterSeq).toBe(9);
        expect(findMany).toHaveBeenCalledWith(expect.objectContaining({
            where: { sessionId: "session-1", seq: { gt: 7 } },
            orderBy: { seq: "asc" },
        }));
    });
});
