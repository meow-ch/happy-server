import { Prisma, PrismaClient } from "@prisma/client";
import { db } from "@/storage/db";

interface SessionMessageStore {
    sessionMessage: Pick<PrismaClient["sessionMessage"], "findMany">;
}

export interface SessionMessageReplayInput {
    sessionId: string;
    afterSeq: number;
    limit: number;
}

export interface SessionMessageReplayPage {
    messages: Array<{
        id: string;
        seq: number;
        localId: string | null;
        content: Prisma.JsonValue;
        createdAt: number;
        updatedAt: number;
    }>;
    hasMore: boolean;
    nextAfterSeq: number;
}

/** Lists messages in durable session sequence order for lossless cursor replay. */
export async function listSessionMessagesAfterSeq(
    store: SessionMessageStore,
    input: SessionMessageReplayInput
): Promise<SessionMessageReplayPage> {
    const rows = await store.sessionMessage.findMany({
        where: {
            sessionId: input.sessionId,
            seq: { gt: input.afterSeq }
        },
        orderBy: { seq: "asc" },
        take: input.limit + 1,
        select: {
            id: true,
            seq: true,
            localId: true,
            content: true,
            createdAt: true,
            updatedAt: true
        }
    });
    const hasMore = rows.length > input.limit;
    const pageRows = hasMore ? rows.slice(0, input.limit) : rows;
    const nextAfterSeq = pageRows.length > 0 ? pageRows[pageRows.length - 1].seq : input.afterSeq;

    return {
        messages: pageRows.map((message) => ({
            id: message.id,
            seq: message.seq,
            localId: message.localId,
            content: message.content,
            createdAt: message.createdAt.getTime(),
            updatedAt: message.updatedAt.getTime()
        })),
        hasMore,
        nextAfterSeq
    };
}

export async function listStoredSessionMessagesAfterSeq(input: SessionMessageReplayInput): Promise<SessionMessageReplayPage> {
    return listSessionMessagesAfterSeq(db, input);
}
