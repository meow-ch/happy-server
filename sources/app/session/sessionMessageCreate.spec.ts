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
    session?: Record<string, unknown>;
} = {}) {
    const message = createMessage();
    return {
        $queryRaw: vi.fn().mockResolvedValue(options.sessionExists === false ? [] : [{
            id: "session-1",
            activeInstanceId: null,
            runtimeConnectionLeaseId: null,
            runtimeConnectionLeaseInstanceId: null,
            runtimeLeaseCurrent: false,
            runtimeReplayLeaseCurrent: false,
            runtimeInstanceRetirementStatus: null,
            targetLegacyRuntimeConnection: false,
            ...options.session,
        }]),
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
                originSocketId: "socket-1",
                targetRuntimeConnectionLeaseId: null,
                targetLegacyRuntimeConnection: false,
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

    it("atomically fences a live runtime write and snapshots its exact delivery lease", async () => {
        const instanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
        const tx = createTx({
            session: {
                activeInstanceId: instanceId,
                runtimeConnectionLeaseId: "lease-1",
                runtimeConnectionLeaseInstanceId: instanceId,
                runtimeLeaseCurrent: true,
                runtimeInstanceRetirementStatus: null,
            },
        });

        const result = await persistSessionMessageInTx(tx as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "lease",
                sessionInstanceId: instanceId,
                leaseId: "lease-1",
            },
        });

        expect(result.result).toBe("success");
        expect(tx.sessionMessageNotificationOutbox.create).toHaveBeenCalledWith({
            data: expect.objectContaining({
                targetRuntimeConnectionLeaseId: "lease-1",
                targetLegacyRuntimeConnection: false,
            }),
        });
    });

    it("marks a lost live lease retryable only while its incarnation remains current and unretired", async () => {
        const instanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
        const current = createTx({
            session: {
                activeInstanceId: instanceId,
                runtimeConnectionLeaseId: "new-lease",
                runtimeConnectionLeaseInstanceId: instanceId,
                runtimeLeaseCurrent: true,
                runtimeInstanceRetirementStatus: null,
            },
        });
        await expect(persistSessionMessageInTx(current as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "lease",
                sessionInstanceId: instanceId,
                leaseId: "old-lease",
            },
        })).resolves.toEqual({
            result: "error",
            code: "runtime_connection_stale",
            retryable: true,
        });

        const superseded = createTx({
            session: {
                activeInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
                runtimeInstanceRetirementStatus: null,
            },
        });
        await expect(persistSessionMessageInTx(superseded as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "lease",
                sessionInstanceId: instanceId,
                leaseId: "old-lease",
            },
        })).resolves.toEqual({
            result: "error",
            code: "runtime_connection_stale",
            retryable: false,
        });
    });

    it("allows exact replay leases, retries an expired replay lease, and terminally rejects displacement", async () => {
        const instanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
        const exact = createTx({
            session: {
                activeInstanceId: instanceId,
                runtimeConnectionLeaseId: "replay-lease",
                runtimeConnectionLeaseInstanceId: instanceId,
                runtimeReplayLeaseCurrent: true,
                runtimeInstanceRetirementStatus: "replaying",
            },
        });
        await expect(persistSessionMessageInTx(exact as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "replay",
                sessionInstanceId: instanceId,
                leaseId: "replay-lease",
            },
        })).resolves.toMatchObject({ result: "success", duplicate: false });

        const expired = createTx({
            session: {
                activeInstanceId: instanceId,
                runtimeConnectionLeaseId: "expired-replay-lease",
                runtimeConnectionLeaseInstanceId: instanceId,
                runtimeReplayLeaseCurrent: false,
                runtimeInstanceRetirementStatus: "replaying",
            },
        });
        await expect(persistSessionMessageInTx(expired as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "replay",
                sessionInstanceId: instanceId,
                leaseId: "expired-replay-lease",
            },
        })).resolves.toEqual({
            result: "error",
            code: "runtime_connection_stale",
            retryable: true,
        });

        const displaced = createTx({
            session: {
                activeInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
                runtimeInstanceRetirementStatus: null,
            },
        });
        await expect(persistSessionMessageInTx(displaced as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: {
                type: "replay",
                sessionInstanceId: instanceId,
                leaseId: "replay-lease",
            },
        })).resolves.toEqual({
            result: "error",
            code: "runtime_connection_stale",
            retryable: false,
        });
    });

    it("lets a terminal replay acknowledge only an exact already-committed local ID", async () => {
        const instanceId = "90b85ebd-6bb8-41bb-aa2d-681765e24f0d";
        const existingMessage = createMessage();
        const duplicate = createTx({
            existingMessage,
            session: {
                activeInstanceId: instanceId,
                runtimeInstanceRetirementStatus: "ended",
            },
        });
        await expect(persistSessionMessageInTx(duplicate as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "ciphertext",
            localId: "local-1",
            runtimeAuthorization: { type: "replay", sessionInstanceId: instanceId },
        })).resolves.toEqual({
            result: "success",
            duplicate: true,
            message: existingMessage,
            updateSeq: null,
        });

        const newOutput = createTx({
            session: {
                activeInstanceId: instanceId,
                runtimeInstanceRetirementStatus: "ended",
            },
        });
        await expect(persistSessionMessageInTx(newOutput as never, {
            userId: "account-1",
            sessionId: "session-1",
            ciphertext: "new-ciphertext",
            localId: "new-local-id",
            runtimeAuthorization: { type: "replay", sessionInstanceId: instanceId },
        })).resolves.toEqual({
            result: "error",
            code: "runtime_connection_stale",
            retryable: false,
        });
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
