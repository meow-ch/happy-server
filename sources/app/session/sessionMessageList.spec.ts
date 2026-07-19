import { describe, expect, it, vi } from "vitest";
import { listSessionMessagesAfterSeq } from "@/app/session/sessionMessageList";

function createMessage(seq: number) {
    return {
        id: `message-${seq}`,
        seq,
        localId: `local-${seq}`,
        content: { t: "encrypted", c: `ciphertext-${seq}` },
        createdAt: new Date(seq * 1000),
        updatedAt: new Date(seq * 1000 + 1)
    };
}

describe("listSessionMessagesAfterSeq", () => {
    it("queries forward from the cursor and exposes the next replay cursor", async () => {
        const findMany = vi.fn().mockResolvedValue([
            createMessage(151),
            createMessage(152),
            createMessage(153)
        ]);

        const result = await listSessionMessagesAfterSeq({ sessionMessage: { findMany } } as never, {
            sessionId: "session-1",
            afterSeq: 150,
            limit: 2
        });

        expect(findMany).toHaveBeenCalledWith({
            where: {
                sessionId: "session-1",
                seq: { gt: 150 }
            },
            orderBy: { seq: "asc" },
            take: 3,
            select: {
                id: true,
                seq: true,
                localId: true,
                content: true,
                createdAt: true,
                updatedAt: true
            }
        });
        expect(result.hasMore).toBe(true);
        expect(result.nextAfterSeq).toBe(152);
        expect(result.messages.map((message) => message.seq)).toEqual([151, 152]);
    });

    it("returns an empty page without advancing the cursor", async () => {
        const findMany = vi.fn().mockResolvedValue([]);

        const result = await listSessionMessagesAfterSeq({ sessionMessage: { findMany } } as never, {
            sessionId: "session-1",
            afterSeq: 900,
            limit: 500
        });

        expect(result).toEqual({
            messages: [],
            hasMore: false,
            nextAfterSeq: 900
        });
        expect(findMany).toHaveBeenCalledWith(expect.objectContaining({
            orderBy: { seq: "asc" },
            take: 501
        }));
    });
});
