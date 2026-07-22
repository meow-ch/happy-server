import fastify from "fastify";
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from "fastify-type-provider-zod";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Fastify } from "../types";

const mocks = vi.hoisted(() => ({
    sessionFindFirst: vi.fn(),
    sessionMessageFindMany: vi.fn()
}));

vi.mock("@/storage/db", () => ({
    db: {
        session: {
            findFirst: mocks.sessionFindFirst
        },
        sessionMessage: {
            findMany: mocks.sessionMessageFindMany
        }
    }
}));

vi.mock("@/app/events/eventRouter", () => ({
    eventRouter: { emitUpdate: vi.fn() },
    buildNewSessionUpdate: vi.fn()
}));

vi.mock("@/storage/seq", () => ({ allocateUserSeq: vi.fn() }));
vi.mock("@/app/session/sessionDelete", () => ({ sessionDelete: vi.fn() }));

import { sessionRoutes } from "./sessionRoutes";

const openApps: Array<ReturnType<typeof fastify>> = [];

async function createApp(userId: string) {
    const app = fastify();
    openApps.push(app);
    app.setValidatorCompiler(validatorCompiler);
    app.setSerializerCompiler(serializerCompiler);
    app.decorate("authenticate", async (request: { userId?: string }) => {
        request.userId = userId;
    });
    sessionRoutes(app.withTypeProvider<ZodTypeProvider>() as unknown as Fastify);
    await app.ready();
    return app;
}

function oldInactiveSession() {
    return {
        id: "session-old",
        seq: 61,
        createdAt: new Date("2026-05-01T10:00:00.000Z"),
        updatedAt: new Date("2026-06-18T12:07:00.000Z"),
        metadata: "encrypted-metadata",
        metadataVersion: 3,
        agentState: "encrypted-agent-state",
        agentStateVersion: 5,
        dataEncryptionKey: new Uint8Array([1, 2, 3, 4]),
        active: false,
        lastActiveAt: new Date("2026-06-18T12:07:00.000Z")
    };
}

describe("GET /v1/sessions/:sessionId", () => {
    beforeEach(() => {
        mocks.sessionFindFirst.mockReset();
        mocks.sessionMessageFindMany.mockReset();
    });

    afterEach(async () => {
        await Promise.all(openApps.splice(0).map((app) => app.close()));
    });

    it("returns an owned old inactive session including its encryption key", async () => {
        mocks.sessionFindFirst.mockResolvedValue(oldInactiveSession());
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old"
        });

        expect(response.statusCode).toBe(200);
        expect(response.headers["cache-control"]).toBe("private, no-store");
        expect(response.headers.vary).toBe("Authorization");
        expect(response.json()).toEqual({
            session: {
                id: "session-old",
                agentStateVersion: 5,
                dataEncryptionKey: Buffer.from([1, 2, 3, 4]).toString("base64")
            }
        });
        expect(mocks.sessionFindFirst).toHaveBeenCalledWith(expect.objectContaining({
            where: { id: "session-old", accountId: "account-owner" }
        }));
        expect(mocks.sessionFindFirst).toHaveBeenCalledWith(expect.objectContaining({
            select: {
                id: true,
                agentStateVersion: true,
                dataEncryptionKey: true
            }
        }));
    });

    it("returns 404 for a session owned by another account", async () => {
        mocks.sessionFindFirst.mockImplementation(async ({ where }: { where: { id: string; accountId: string } }) => {
            return where.id === "session-old" && where.accountId === "account-owner"
                ? oldInactiveSession()
                : null;
        });
        const app = await createApp("account-attacker");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old"
        });

        expect(response.statusCode).toBe(404);
        expect(response.headers["cache-control"]).toBe("private, no-store");
        expect(response.headers.vary).toBe("Authorization");
        expect(response.json()).toEqual({
            error: "Session not found",
            code: "SESSION_NOT_FOUND"
        });
        expect(mocks.sessionFindFirst).toHaveBeenCalledWith(expect.objectContaining({
            where: { id: "session-old", accountId: "account-attacker" }
        }));
    });

    it("does not silently serialize an omitted encryption-key field as null", async () => {
        const session: Record<string, unknown> = { ...oldInactiveSession() };
        delete session.dataEncryptionKey;
        mocks.sessionFindFirst.mockResolvedValue(session);
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old"
        });

        expect(response.statusCode).toBe(500);
    });

    it("returns 404 for an unknown session", async () => {
        mocks.sessionFindFirst.mockResolvedValue(null);
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/missing"
        });

        expect(response.statusCode).toBe(404);
        expect(response.json()).toEqual({
            error: "Session not found",
            code: "SESSION_NOT_FOUND"
        });
    });

    it("does not shadow the more specific session messages route", async () => {
        mocks.sessionFindFirst.mockResolvedValue(oldInactiveSession());
        mocks.sessionMessageFindMany.mockResolvedValue([]);
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old/messages"
        });

        expect(response.statusCode).toBe(200);
        expect(response.json()).toEqual({ messages: [] });
        expect(mocks.sessionMessageFindMany).toHaveBeenCalledWith(expect.objectContaining({
            where: { sessionId: "session-old" }
        }));
    });
});
