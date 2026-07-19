import { describe, expect, it, vi } from "vitest";
import { persistSessionMessageInTx, toSessionMessageAck } from "@/app/session/sessionMessageCreate";
import { SessionMessage } from "@prisma/client";

function createMessage(overrides: Record<string, unknown> = {}): SessionMessage {
    return {
        id: "message-1",
        sessionId: "session-1",
        localId: "local-1",
        seq: 7,
        content: { t: "encrypted", c: "ciphertext" },
        createdAt: new Date("2026-07-19T10:00:00.000Z"),
        updatedAt: new Date("2026-07-19T10:00:01.000Z"),
        ...overrides
    } as SessionMessage;
}

function createTx(options: {
    sessionExists?: boolean;
    existingMessage?: ReturnType<typeof createMessage> | null;
} = {}) {
    const message = createMessage();
    return {
        $queryRaw: vi.fn().mockResolvedValue(options.sessionExists === false ? [] : [{ id: "session-1" }]),
        session: {
            update: vi.fn().mockResolvedValue({ seq: message.seq })
        },
        account: {
            update: vi.fn().mockResolvedValue({ seq: 31 })
        },
        sessionMessage: {
            findUnique: vi.fn().mockResolvedValue(options.existingMessage ?? null),
            create: vi.fn().mockResolvedValue(message)
        },
        sessionMessageNotificationOutbox: {
            create: vi.fn().mockResolvedValue({ id: "notification-1" })
        }
    };
}

describe("persistSessionMessageInTx", () => {
    it("locks the session and atomically creates the message and notification", async () => {
        const tx = createTx();

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            originSocketId: "socket-1"
        });

        expect(result).toMatchObject({
            result: "success",
            duplicate: false,
            updateSeq: 31,
            message: { id: "message-1", seq: 7, localId: "local-1" }
        });
        expect(tx.$queryRaw).toHaveBeenCalledTimes(1);
        expect(tx.account.update).toHaveBeenCalledWith(expect.objectContaining({
            where: { id: "account-1" },
            data: { seq: { increment: 1 } }
        }));
        expect(tx.session.update).toHaveBeenCalledWith(expect.objectContaining({
            where: { id: "session-1" },
            data: { seq: { increment: 1 } }
        }));
        expect(tx.sessionMessage.create).toHaveBeenCalledWith({
            data: {
                sessionId: "session-1",
                seq: 7,
                content: { t: "encrypted", c: "ciphertext" },
                localId: "local-1"
            }
        });
        expect(tx.sessionMessageNotificationOutbox.create).toHaveBeenCalledWith({
            data: {
                accountId: "account-1",
                sessionId: "session-1",
                messageId: "message-1",
                updateSeq: 31,
                originSocketId: "socket-1"
            }
        });
        expect(tx.sessionMessage.create.mock.invocationCallOrder[0])
            .toBeLessThan(tx.account.update.mock.invocationCallOrder[0]);
    });

    it("returns the committed row as a duplicate without consuming sequences", async () => {
        const existingMessage = createMessage();
        const tx = createTx({ existingMessage });

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1"
        });

        expect(result).toEqual({
            result: "success",
            duplicate: true,
            message: existingMessage,
            updateSeq: null
        });
        expect(tx.account.update).not.toHaveBeenCalled();
        expect(tx.session.update).not.toHaveBeenCalled();
        expect(tx.sessionMessage.create).not.toHaveBeenCalled();
        expect(tx.sessionMessageNotificationOutbox.create).not.toHaveBeenCalled();
    });

    it("does not recreate a notification for a duplicate after its delivered row was cleaned up", async () => {
        const existingMessage = createMessage();
        const tx = createTx({ existingMessage });

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            originSocketId: "replacement-socket"
        });

        expect(result).toEqual({
            result: "success",
            duplicate: true,
            message: existingMessage,
            updateSeq: null
        });
        expect(tx.account.update).not.toHaveBeenCalled();
        expect(tx.sessionMessageNotificationOutbox.create).not.toHaveBeenCalled();
    });

    it("rejects reuse of a local ID with different ciphertext", async () => {
        const tx = createTx({ existingMessage: createMessage({ content: { t: "encrypted", c: "other" } }) });

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1"
        });

        expect(result).toEqual({
            result: "error",
            code: "idempotency_conflict",
            retryable: false
        });
        expect(tx.account.update).not.toHaveBeenCalled();
        expect(tx.sessionMessage.create).not.toHaveBeenCalled();
        expect(tx.sessionMessageNotificationOutbox.create).not.toHaveBeenCalled();
    });

    it("rejects a session that is not owned by the authenticated account", async () => {
        const tx = createTx({ sessionExists: false });

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1"
        });

        expect(result).toEqual({
            result: "error",
            code: "session_not_found",
            retryable: false
        });
        expect(tx.sessionMessage.findUnique).not.toHaveBeenCalled();
        expect(tx.sessionMessageNotificationOutbox.create).not.toHaveBeenCalled();
    });
});

describe("toSessionMessageAck", () => {
    it("uses the canonical nested ACK envelope", () => {
        expect(toSessionMessageAck({
            result: "success",
            duplicate: true,
            message: createMessage(),
            updateSeq: null
        })).toEqual({
            result: "success",
            duplicate: true,
            message: {
                id: "message-1",
                seq: 7,
                localId: "local-1",
                createdAt: 1784455200000,
                updatedAt: 1784455201000
            }
        });
    });
});
