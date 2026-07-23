import fastify from "fastify";
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from "fastify-type-provider-zod";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Fastify } from "../types";

const mocks = vi.hoisted(() => ({
    sessionFindFirst: vi.fn(),
    sessionCreate: vi.fn(),
    sessionMessageFindMany: vi.fn(),
    allocateUserSeq: vi.fn(),
    emitUpdate: vi.fn(),
    buildNewSessionUpdate: vi.fn(),
    findRuntimeConnectionSession: vi.fn(),
}));

vi.mock("@/storage/db", () => ({
    db: {
        session: {
            findFirst: mocks.sessionFindFirst,
            create: mocks.sessionCreate
        },
        sessionMessage: {
            findMany: mocks.sessionMessageFindMany
        }
    }
}));

vi.mock("@/app/events/eventRouter", () => ({
    eventRouter: { emitUpdate: mocks.emitUpdate },
    buildNewSessionUpdate: mocks.buildNewSessionUpdate
}));

vi.mock("@/storage/seq", () => ({ allocateUserSeq: mocks.allocateUserSeq }));
vi.mock("@/app/session/sessionDelete", () => ({ sessionDelete: vi.fn() }));
vi.mock("@/app/presence/runtimeConnectionLease", () => ({
    findRuntimeConnectionSession: mocks.findRuntimeConnectionSession,
}));

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
        activeInstanceId: null,
        lastActiveAt: new Date("2026-06-18T12:07:00.000Z")
    };
}

function newSession(overrides: Record<string, unknown> = {}) {
    return {
        id: "session-new",
        seq: 0,
        createdAt: new Date("2026-07-22T15:24:11.000Z"),
        updatedAt: new Date("2026-07-22T15:24:11.000Z"),
        metadata: "encrypted-metadata",
        metadataVersion: 0,
        agentState: null,
        agentStateVersion: 0,
        dataEncryptionKey: null,
        active: true,
        activeInstanceId: null,
        lastActiveAt: new Date("2026-07-22T15:24:11.000Z"),
        ...overrides
    };
}

describe("POST /v1/sessions", () => {
    beforeEach(() => {
        mocks.sessionFindFirst.mockReset();
        mocks.sessionCreate.mockReset();
        mocks.allocateUserSeq.mockReset();
        mocks.emitUpdate.mockReset();
        mocks.buildNewSessionUpdate.mockReset();
        mocks.allocateUserSeq.mockResolvedValue(42);
        mocks.buildNewSessionUpdate.mockReturnValue({ type: "new-session" });
    });

    afterEach(async () => {
        await Promise.all(openApps.splice(0).map((app) => app.close()));
    });

    it("persists a supplied encrypted initial agent state at version one", async () => {
        const encryptedKey = Buffer.from([9, 8, 7, 6]).toString("base64");
        mocks.sessionFindFirst.mockResolvedValue(null);
        mocks.sessionCreate.mockResolvedValue(newSession({
            agentState: "encrypted-initial-state",
            agentStateVersion: 1,
            dataEncryptionKey: new Uint8Array([9, 8, 7, 6])
        }));
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "POST",
            url: "/v1/sessions",
            payload: {
                tag: "durable-session-tag",
                metadata: "encrypted-metadata",
                agentState: "encrypted-initial-state",
                dataEncryptionKey: encryptedKey
            }
        });

        expect(response.statusCode).toBe(200);
        expect(mocks.sessionCreate).toHaveBeenCalledWith({
            data: {
                accountId: "account-owner",
                tag: "durable-session-tag",
                metadata: "encrypted-metadata",
                agentState: "encrypted-initial-state",
                agentStateVersion: 1,
                dataEncryptionKey: new Uint8Array([9, 8, 7, 6])
            }
        });
        expect(response.json().session).toMatchObject({
            id: "session-new",
            agentState: "encrypted-initial-state",
            agentStateVersion: 1,
            dataEncryptionKey: encryptedKey
        });
        expect(mocks.emitUpdate).toHaveBeenCalledTimes(1);
    });

    it.each([
        ["omitted", {}],
        ["null", { agentState: null }]
    ])("persists null agent state at version zero when it is %s", async (_label, extraPayload) => {
        mocks.sessionFindFirst.mockResolvedValue(null);
        mocks.sessionCreate.mockResolvedValue(newSession());
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "POST",
            url: "/v1/sessions",
            payload: {
                tag: "durable-session-tag",
                metadata: "encrypted-metadata",
                ...extraPayload
            }
        });

        expect(response.statusCode).toBe(200);
        expect(mocks.sessionCreate).toHaveBeenCalledWith({
            data: {
                accountId: "account-owner",
                tag: "durable-session-tag",
                metadata: "encrypted-metadata",
                agentState: null,
                agentStateVersion: 0,
                dataEncryptionKey: undefined
            }
        });
        expect(response.json().session).toMatchObject({
            agentState: null,
            agentStateVersion: 0
        });
    });

    it("returns an existing tagged session without overwriting its state", async () => {
        mocks.sessionFindFirst.mockResolvedValue(newSession({
            id: "session-existing",
            metadata: "original-metadata",
            agentState: "original-agent-state",
            agentStateVersion: 7,
            dataEncryptionKey: new Uint8Array([1, 2, 3, 4])
        }));
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "POST",
            url: "/v1/sessions",
            payload: {
                tag: "durable-session-tag",
                metadata: "replacement-metadata",
                agentState: "replacement-agent-state",
                dataEncryptionKey: Buffer.from([9, 9, 9, 9]).toString("base64")
            }
        });

        expect(response.statusCode).toBe(200);
        expect(response.json().session).toMatchObject({
            id: "session-existing",
            metadata: "original-metadata",
            agentState: "original-agent-state",
            agentStateVersion: 7,
            dataEncryptionKey: Buffer.from([1, 2, 3, 4]).toString("base64")
        });
        expect(mocks.sessionCreate).not.toHaveBeenCalled();
        expect(mocks.allocateUserSeq).not.toHaveBeenCalled();
        expect(mocks.emitUpdate).not.toHaveBeenCalled();
    });
});

describe("GET /v1/sessions/:sessionId", () => {
    beforeEach(() => {
        mocks.sessionFindFirst.mockReset();
        mocks.sessionMessageFindMany.mockReset();
        mocks.findRuntimeConnectionSession.mockReset();
    });

    afterEach(async () => {
        await Promise.all(openApps.splice(0).map((app) => app.close()));
    });

    it("returns an owned old inactive session including its encryption key", async () => {
        mocks.findRuntimeConnectionSession.mockResolvedValue({
            id: "session-old",
            agentStateVersion: 5,
            runtimeConnectionProtocolVersion: 1,
            runtimeConnected: false,
            runtimeInstanceId: null,
            runtimeLeaseInstanceId: null,
            runtimeLeaseExpiresAt: null,
            runtimeInstanceRetired: false,
            runtimeConnectionCheckedAt: new Date("2026-07-22T18:00:00.000Z"),
            dataEncryptionKey: new Uint8Array([1, 2, 3, 4]),
        });
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
                runtimeConnectionProtocolVersion: 1,
                runtimeConnected: false,
                runtimeInstanceId: null,
                runtimeLeaseInstanceId: null,
                runtimeLeaseExpiresAt: null,
                runtimeInstanceRetired: false,
                runtimeConnectionCheckedAt: new Date("2026-07-22T18:00:00.000Z").getTime(),
                dataEncryptionKey: Buffer.from([1, 2, 3, 4]).toString("base64")
            }
        });
        expect(mocks.findRuntimeConnectionSession)
            .toHaveBeenCalledWith("account-owner", "session-old");
    });

    it("reports a claimed CLI session incarnation as runtime connected", async () => {
        mocks.findRuntimeConnectionSession.mockResolvedValue({
            id: "session-old",
            agentStateVersion: 5,
            runtimeConnectionProtocolVersion: 1,
            runtimeConnected: true,
            runtimeInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
            runtimeLeaseInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
            runtimeLeaseExpiresAt: new Date("2026-07-22T18:02:00.000Z"),
            runtimeInstanceRetired: false,
            runtimeConnectionCheckedAt: new Date("2026-07-22T18:00:00.000Z"),
            dataEncryptionKey: new Uint8Array([1, 2, 3, 4]),
        });
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old"
        });

        expect(response.statusCode).toBe(200);
        expect(response.json().session.runtimeConnected).toBe(true);
    });

    it("returns 404 for a session owned by another account", async () => {
        mocks.findRuntimeConnectionSession.mockImplementation(async (accountId: string, sessionId: string) => {
            return sessionId === "session-old" && accountId === "account-owner"
                ? {
                    id: "session-old",
                    agentStateVersion: 5,
                    runtimeConnectionProtocolVersion: 1,
                    runtimeConnected: false,
                    runtimeInstanceId: null,
                    runtimeLeaseInstanceId: null,
                    runtimeLeaseExpiresAt: null,
                    runtimeInstanceRetired: false,
                    runtimeConnectionCheckedAt: new Date("2026-07-22T18:00:00.000Z"),
                    dataEncryptionKey: new Uint8Array([1, 2, 3, 4]),
                }
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
        expect(mocks.findRuntimeConnectionSession)
            .toHaveBeenCalledWith("account-attacker", "session-old");
    });

    it("does not silently serialize an omitted encryption-key field as null", async () => {
        const session: Record<string, unknown> = {
            id: "session-old",
            agentStateVersion: 5,
            runtimeConnectionProtocolVersion: 1,
            runtimeConnected: false,
            runtimeInstanceId: null,
            runtimeLeaseInstanceId: null,
            runtimeLeaseExpiresAt: null,
            runtimeInstanceRetired: false,
            runtimeConnectionCheckedAt: new Date("2026-07-22T18:00:00.000Z"),
            dataEncryptionKey: new Uint8Array([1, 2, 3, 4]),
        };
        delete session.dataEncryptionKey;
        mocks.findRuntimeConnectionSession.mockResolvedValue(session);
        const app = await createApp("account-owner");

        const response = await app.inject({
            method: "GET",
            url: "/v1/sessions/session-old"
        });

        expect(response.statusCode).toBe(500);
    });

    it("returns 404 for an unknown session", async () => {
        mocks.findRuntimeConnectionSession.mockResolvedValue(null);
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
