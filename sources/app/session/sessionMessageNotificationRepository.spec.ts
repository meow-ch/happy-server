import { beforeEach, describe, expect, it, vi } from "vitest";

const dbMocks = vi.hoisted(() => ({
    queryRaw: vi.fn(),
    updateMany: vi.fn(),
    findUnique: vi.fn(),
    deleteMany: vi.fn()
}));

vi.mock("@/storage/db", () => ({
    db: {
        $queryRaw: dbMocks.queryRaw,
        sessionMessageNotificationOutbox: dbMocks
    }
}));
vi.mock("@/storage/files", () => ({
    getPublicUrl: (path: string) => path
}));

import { sessionMessageNotificationRepository } from "@/app/session/sessionMessageNotificationOutbox";

describe("sessionMessageNotificationRepository", () => {
    beforeEach(() => {
        for (const mock of Object.values(dbMocks)) mock.mockReset();
    });

    it("atomically wins a pending lease so another pod cannot claim the same row", async () => {
        let claimToken = "";
        dbMocks.queryRaw.mockResolvedValue([{ id: "notification-1" }]);
        dbMocks.updateMany.mockImplementation(async (args: any) => {
            claimToken = args.data.claimToken;
            return { count: 1 };
        });
        dbMocks.findUnique.mockImplementation(async () => ({
            id: "notification-1",
            accountId: "account-1",
            sessionId: "session-1",
            messageId: "message-1",
            updateSeq: 31,
            originSocketId: "socket-1",
            attempts: 1,
            claimToken,
            message: {
                id: "message-1",
                seq: 7,
                content: { t: "encrypted", c: "ciphertext" },
                localId: null,
                createdAt: new Date(),
                updatedAt: new Date()
            }
        }));
        const now = new Date("2026-07-19T10:00:00.000Z");

        const claimed = await sessionMessageNotificationRepository.claimBatch(now, 30_000, 25);

        expect(claimed).toHaveLength(1);
        expect(claimToken).toMatch(/^[0-9a-f-]{36}$/);
        const query = dbMocks.queryRaw.mock.calls[0][0];
        expect(query.sql).toContain('NOT EXISTS');
        expect(query.sql).toContain('earlier."sessionId" = candidate."sessionId"');
        expect(query.sql).toContain('earlier_message."seq" < candidate_message."seq"');
        expect(dbMocks.updateMany).toHaveBeenCalledWith(expect.objectContaining({
            where: expect.objectContaining({
                id: "notification-1",
                deliveredAt: null,
                nextAttemptAt: { lte: now }
            }),
            data: expect.objectContaining({
                claimedAt: now,
                claimToken,
                attempts: { increment: 1 }
            })
        }));

        dbMocks.updateMany.mockResolvedValue({ count: 0 });
        await expect(sessionMessageNotificationRepository.claimBatch(now, 30_000, 25))
            .resolves.toEqual([]);
    });

    it("never exposes a later session row while an earlier row is undelivered or leased", async () => {
        dbMocks.queryRaw.mockResolvedValue([]);
        const now = new Date("2026-07-19T10:00:00.000Z");

        await expect(sessionMessageNotificationRepository.claimBatch(now, 30_000, 25))
            .resolves.toEqual([]);

        const query = dbMocks.queryRaw.mock.calls[0][0];
        expect(query.sql).toContain('candidate."deliveredAt" IS NULL');
        expect(query.sql).toContain('earlier."deliveredAt" IS NULL');
        expect(query.sql).not.toContain('earlier."nextAttemptAt"');
        expect(dbMocks.updateMany).not.toHaveBeenCalled();
    });
});
