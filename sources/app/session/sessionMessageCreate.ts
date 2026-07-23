import { Prisma, SessionMessage } from "@prisma/client";
import { inTx, Tx } from "@/storage/inTx";
import { LEGACY_RUNTIME_CONNECTION_GRACE_MS } from "@/app/presence/runtimeConnectionLease";

export type RuntimeMessageAuthorization =
    | { type: "lease"; sessionInstanceId: string; leaseId: string }
    | { type: "legacy" }
    | { type: "replay"; sessionInstanceId?: string; leaseId?: string };

export interface PersistSessionMessageInput {
    userId: string;
    sessionId: string;
    ciphertext: string;
    localId: string | null;
    originSocketId?: string | null;
    runtimeAuthorization?: RuntimeMessageAuthorization;
}

export interface PersistSessionMessageSuccess {
    result: "success";
    duplicate: boolean;
    message: SessionMessage;
    updateSeq: number | null;
}

export interface PersistSessionMessageError {
    result: "error";
    code: "session_not_found" | "idempotency_conflict" | "runtime_connection_stale";
    retryable: boolean;
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
    code: "invalid_request" | "session_not_found" | "idempotency_conflict" | "runtime_connection_stale" | "internal_error";
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
    const sessions = await tx.$queryRaw<Array<{
        id: string;
        activeInstanceId: string | null;
        runtimeConnectionLeaseId: string | null;
        runtimeConnectionLeaseInstanceId: string | null;
        runtimeLeaseCurrent: boolean;
        runtimeReplayLeaseCurrent: boolean;
        runtimeInstanceRetirementStatus: string | null;
        targetLegacyRuntimeConnection: boolean;
    }>>(Prisma.sql`
        WITH runtime_lock AS MATERIALIZED (
            SELECT pg_advisory_xact_lock(hashtextextended(${input.sessionId}, 0))
        )
        SELECT session."id",
               session."activeInstanceId",
               session."runtimeConnectionLeaseId",
               session."runtimeConnectionLeaseInstanceId",
               (
                    session."active"
                AND session."activeInstanceId" IS NOT NULL
                AND session."runtimeConnectionLeaseId" IS NOT NULL
                AND session."runtimeConnectionLeaseInstanceId" = session."activeInstanceId"
                AND session."runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP
                AND NOT EXISTS (
                    SELECT 1
                    FROM "SessionRuntimeInstanceRetirement" retirement
                    WHERE retirement."sessionId" = session."id"
                      AND retirement."instanceId" = session."activeInstanceId"
                )
               ) AS "runtimeLeaseCurrent",
               (
                    session."activeInstanceId" IS NOT NULL
                AND session."runtimeConnectionLeaseId" IS NOT NULL
                AND session."runtimeConnectionLeaseInstanceId" = session."activeInstanceId"
                AND session."runtimeConnectionLeaseExpiresAt" > CURRENT_TIMESTAMP
                AND EXISTS (
                    SELECT 1
                    FROM "SessionRuntimeInstanceRetirement" retirement
                    WHERE retirement."sessionId" = session."id"
                      AND retirement."instanceId" = session."activeInstanceId"
                      AND retirement."status" = 'replaying'
                )
               ) AS "runtimeReplayLeaseCurrent",
               (
                    SELECT retirement."status"
                    FROM "SessionRuntimeInstanceRetirement" retirement
                    WHERE retirement."sessionId" = session."id"
                      AND retirement."instanceId" = session."activeInstanceId"
                    LIMIT 1
               ) AS "runtimeInstanceRetirementStatus",
               (
                    session."active"
                AND session."runtimeConnectionLeaseId" IS NULL
                AND session."runtimeConnectionLeaseInstanceId" IS NULL
                AND session."runtimeConnectionLeaseExpiresAt" IS NULL
                AND session."lastActiveAt" > CURRENT_TIMESTAMP
                    - (${LEGACY_RUNTIME_CONNECTION_GRACE_MS} * INTERVAL '1 millisecond')
               ) AS "targetLegacyRuntimeConnection"
        FROM "Session" session
        CROSS JOIN runtime_lock
        WHERE session."id" = ${input.sessionId}
          AND session."accountId" = ${input.userId}
        FOR UPDATE OF session
    `);
    if (sessions.length === 0) {
        return {
            result: "error",
            code: "session_not_found",
            retryable: false
        };
    }

    const session = sessions[0];
    const existingMessage = input.localId
        ? await tx.sessionMessage.findUnique({
            where: {
                sessionId_localId: {
                    sessionId: input.sessionId,
                    localId: input.localId
                }
            }
        })
        : null;

    if (input.runtimeAuthorization) {
        const authorization = input.runtimeAuthorization;
        const authorized = authorization.type === "lease"
            ? session.runtimeLeaseCurrent
                && session.activeInstanceId === authorization.sessionInstanceId
                && session.runtimeConnectionLeaseInstanceId === authorization.sessionInstanceId
                && session.runtimeConnectionLeaseId === authorization.leaseId
            : authorization.type === "legacy"
                ? session.activeInstanceId === null
                    && session.targetLegacyRuntimeConnection
                : authorization.sessionInstanceId !== undefined
                    && authorization.leaseId !== undefined
                    && session.runtimeReplayLeaseCurrent
                    && session.activeInstanceId === authorization.sessionInstanceId
                    && session.runtimeConnectionLeaseInstanceId === authorization.sessionInstanceId
                    && session.runtimeConnectionLeaseId === authorization.leaseId;

        if (!authorized) {
            // A retired replay producer may have lost the ACK for an already
            // committed local ID. Returning that exact duplicate is harmless;
            // creating any new output would resurrect a superseded runtime.
            if (authorization.type === "replay"
                && existingMessage
                && hasCiphertext(existingMessage.content, input.ciphertext)) {
                return {
                    result: "success",
                    duplicate: true,
                    message: existingMessage,
                    updateSeq: null,
                };
            }
            return {
                result: "error",
                code: "runtime_connection_stale",
                retryable: authorization.type === "lease"
                    ? session.activeInstanceId === authorization.sessionInstanceId
                        && session.runtimeInstanceRetirementStatus === null
                    : authorization.type === "replay"
                        ? authorization.sessionInstanceId !== undefined
                            && session.activeInstanceId === authorization.sessionInstanceId
                            && session.runtimeInstanceRetirementStatus === "replaying"
                        : false,
            };
        }
    }

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
            originSocketId: input.originSocketId ?? null,
            targetRuntimeConnectionLeaseId: session.runtimeLeaseCurrent === true
                ? session.runtimeConnectionLeaseId
                : null,
            targetLegacyRuntimeConnection: session.targetLegacyRuntimeConnection === true,
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
