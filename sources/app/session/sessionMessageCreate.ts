import { Prisma, SessionMessage } from "@prisma/client";
import { inTx, Tx } from "@/storage/inTx";

export interface PersistSessionMessageInput {
    userId: string;
    sessionId: string;
    ciphertext: string;
    localId: string | null;
    originSocketId?: string | null;
}

export interface PersistSessionMessageSuccess {
    result: "success";
    duplicate: boolean;
    message: SessionMessage;
    updateSeq: number | null;
}

export interface PersistSessionMessageError {
    result: "error";
    code: "session_not_found" | "idempotency_conflict";
    retryable: false;
}

export type PersistSessionMessageResult = PersistSessionMessageSuccess | PersistSessionMessageError;

export interface SessionMessageSuccessAck {
    result: "success";
    duplicate: boolean;
    message: {
        id: string;
        seq: number;
        localId: string | null;
        createdAt: number;
        updatedAt: number;
    };
}

export interface SessionMessageErrorAck {
    result: "error";
    code: "invalid_request" | "session_not_found" | "idempotency_conflict" | "internal_error";
    retryable: boolean;
}

export type SessionMessageAck = SessionMessageSuccessAck | SessionMessageErrorAck;

function hasCiphertext(content: Prisma.JsonValue, ciphertext: string): boolean {
    if (!content || Array.isArray(content) || typeof content !== "object") {
        return false;
    }
    return content.t === "encrypted" && content.c === ciphertext;
}

/**
 * Persists one encrypted session message inside the caller's transaction.
 * The session row lock preserves message order and serializes local-ID checks
 * across server replicas. The account sequence is allocated only after the
 * potentially large message insert so its account-wide lock is held briefly.
 */
export async function persistSessionMessageInTx(
    tx: Tx,
    input: PersistSessionMessageInput
): Promise<PersistSessionMessageResult> {
    const sessions = await tx.$queryRaw<Array<{ id: string }>>`
        SELECT "id" FROM "Session"
        WHERE "id" = ${input.sessionId} AND "accountId" = ${input.userId}
        FOR UPDATE
    `;
    if (sessions.length === 0) {
        return {
            result: "error",
            code: "session_not_found",
            retryable: false
        };
    }

    if (input.localId) {
        const existingMessage = await tx.sessionMessage.findUnique({
            where: {
                sessionId_localId: {
                    sessionId: input.sessionId,
                    localId: input.localId
                }
            }
        });
        if (existingMessage) {
            if (!hasCiphertext(existingMessage.content, input.ciphertext)) {
                return {
                    result: "error",
                    code: "idempotency_conflict",
                    retryable: false
                };
            }
            // Never recreate notification state for a duplicate. Delivered
            // outbox rows are retained only temporarily, so absence cannot
            // distinguish a pre-outbox message from a row already delivered
            // and cleaned up. Cursor replay repairs any missed live wakeup.
            return {
                result: "success",
                duplicate: true,
                message: existingMessage,
                updateSeq: null
            };
        }
    }

    const updatedSession = await tx.session.update({
        where: { id: input.sessionId },
        select: { seq: true },
        data: { seq: { increment: 1 } }
    });
    const message = await tx.sessionMessage.create({
        data: {
            sessionId: input.sessionId,
            seq: updatedSession.seq,
            content: {
                t: "encrypted",
                c: input.ciphertext
            },
            localId: input.localId
        }
    });
    const account = await tx.account.update({
        where: { id: input.userId },
        select: { seq: true },
        data: { seq: { increment: 1 } }
    });
    await tx.sessionMessageNotificationOutbox.create({
        data: {
            accountId: input.userId,
            sessionId: input.sessionId,
            messageId: message.id,
            updateSeq: account.seq,
            originSocketId: input.originSocketId ?? null
        }
    });

    return {
        result: "success",
        duplicate: false,
        message,
        updateSeq: account.seq
    };
}

export async function persistSessionMessage(input: PersistSessionMessageInput): Promise<PersistSessionMessageResult> {
    return inTx((tx) => persistSessionMessageInTx(tx, input));
}

export function toSessionMessageAck(result: PersistSessionMessageResult): SessionMessageAck {
    if (result.result === "error") {
        return result;
    }
    return {
        result: "success",
        duplicate: result.duplicate,
        message: {
            id: result.message.id,
            seq: result.message.seq,
            localId: result.message.localId,
            createdAt: result.message.createdAt.getTime(),
            updatedAt: result.message.updatedAt.getTime()
        }
    };
}
