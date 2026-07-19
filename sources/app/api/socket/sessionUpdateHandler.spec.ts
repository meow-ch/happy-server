import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    persistSessionMessage: vi.fn(),
    toSessionMessageAck: vi.fn((result: any) => ({ result: result.result })),
    log: vi.fn(),
    sessionFindUnique: vi.fn(),
    sessionUpdateMany: vi.fn(),
    emitEphemeral: vi.fn()
}));

vi.mock("@/app/monitoring/metrics2", () => ({
    sessionAliveEventsCounter: { inc: vi.fn() },
    websocketEventsCounter: { inc: vi.fn() }
}));
vi.mock("@/app/presence/sessionCache", () => ({
    activityCache: { isSessionValid: vi.fn(), queueSessionUpdate: vi.fn() }
}));
vi.mock("@/app/events/eventRouter", () => ({
    buildSessionActivityEphemeral: vi.fn(),
    buildUpdateSessionUpdate: vi.fn(),
    eventRouter: { emitUpdate: vi.fn(), emitEphemeral: mocks.emitEphemeral }
}));
vi.mock("@/storage/db", () => ({
    db: {
        session: {
            findUnique: mocks.sessionFindUnique,
            updateMany: mocks.sessionUpdateMany
        }
    }
}));
vi.mock("@/storage/seq", () => ({ allocateUserSeq: vi.fn() }));
vi.mock("@/utils/log", () => ({ log: mocks.log }));
vi.mock("@/utils/randomKeyNaked", () => ({ randomKeyNaked: vi.fn(() => "update-1") }));
vi.mock("@/app/session/sessionMessageCreate", () => ({
    persistSessionMessage: mocks.persistSessionMessage,
    toSessionMessageAck: mocks.toSessionMessageAck
}));

import {
    MAX_SESSION_MESSAGE_CIPHERTEXT_BYTES,
    sessionUpdateHandler
} from "@/app/api/socket/sessionUpdateHandler";

function success(id: string, seq: number) {
    return {
        result: "success",
        duplicate: false,
        updateSeq: seq,
        message: {
            id,
            sessionId: "session-1",
            localId: null,
            seq,
            content: { t: "encrypted", c: id },
            createdAt: new Date(),
            updatedAt: new Date()
        }
    };
}

function createHarness() {
    const handlers = new Map<string, (...args: any[]) => any>();
    const socket = {
        id: "socket-1",
        on: vi.fn((event: string, handler: (...args: any[]) => any) => handlers.set(event, handler))
    };
    const dispatcher = {
        wake: vi.fn()
    };
    sessionUpdateHandler(
        "account-1",
        socket as never,
        { connectionType: "user-scoped", socket, userId: "account-1" } as never,
        dispatcher as never
    );
    return { handlers, dispatcher };
}

describe("session message socket handler", () => {
    beforeEach(() => {
        mocks.persistSessionMessage.mockReset();
        mocks.toSessionMessageAck.mockClear();
        mocks.log.mockClear();
        mocks.sessionFindUnique.mockReset();
        mocks.sessionUpdateMany.mockReset();
        mocks.emitEphemeral.mockReset();
    });

    it("preserves socket receive order while asynchronous commits are in flight", async () => {
        let resolveFirst!: (value: ReturnType<typeof success>) => void;
        mocks.persistSessionMessage
            .mockImplementationOnce(() => new Promise((resolve) => { resolveFirst = resolve; }))
            .mockResolvedValueOnce(success("message-2", 2));
        const { handlers, dispatcher } = createHarness();
        const handleMessage = handlers.get("message")!;

        const first = handleMessage({ sid: "session-1", message: "ciphertext-1" }, vi.fn());
        const second = handleMessage({ sid: "session-1", message: "ciphertext-2" }, vi.fn());
        await Promise.resolve();

        expect(mocks.persistSessionMessage).toHaveBeenCalledTimes(1);
        resolveFirst(success("message-1", 1));
        await Promise.all([first, second]);

        expect(mocks.persistSessionMessage.mock.calls.map((call) => call[0].ciphertext))
            .toEqual(["ciphertext-1", "ciphertext-2"]);
        expect(dispatcher.wake).toHaveBeenCalledTimes(2);
    });

    it("rejects a non-UUID local ID without persisting or logging it", async () => {
        const { handlers } = createHarness();
        const callback = vi.fn();

        await handlers.get("message")?.({
            sid: "session-1",
            message: "ciphertext",
            localId: "attacker-controlled-value"
        }, callback);

        expect(mocks.persistSessionMessage).not.toHaveBeenCalled();
        expect(mocks.log).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            result: "error",
            code: "invalid_request",
            retryable: false
        });
    });

    it("rejects an oversized encrypted message before database or Redis work", async () => {
        const { handlers, dispatcher } = createHarness();
        const callback = vi.fn();

        await handlers.get("message")?.({
            sid: "session-1",
            message: "x".repeat(MAX_SESSION_MESSAGE_CIPHERTEXT_BYTES + 1)
        }, callback);

        expect(mocks.persistSessionMessage).not.toHaveBeenCalled();
        expect(dispatcher.wake).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            result: "error",
            code: "invalid_request",
            retryable: false
        });
    });

    it("logs only the presence of a valid UUID, not the raw identifier", async () => {
        const localId = "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3";
        mocks.persistSessionMessage.mockResolvedValue(success("message-1", 1));
        const { handlers } = createHarness();

        await handlers.get("message")?.({ sid: "session-1", message: "ciphertext", localId }, vi.fn());

        const renderedLog = JSON.stringify(mocks.log.mock.calls);
        expect(renderedLog).toContain("hasLocalId=true");
        expect(renderedLog).not.toContain(localId);
    });

    it("ACKs a duplicate producer retry without reopening its delivered notification", async () => {
        mocks.persistSessionMessage.mockResolvedValue({
            ...success("message-1", 1),
            duplicate: true
        });
        const { handlers, dispatcher } = createHarness();
        const callback = vi.fn();

        await handlers.get("message")?.({
            sid: "session-1",
            message: "ciphertext",
            localId: "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3"
        }, callback);

        expect(dispatcher.wake).toHaveBeenCalledTimes(1);
        expect(callback).toHaveBeenCalledWith({ result: "success" });
    });

    it("acknowledges session-end only after the inactive state is persisted", async () => {
        const localId = "16cc3995-c10f-43c0-8dbd-95ef41f924a7";
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        mocks.sessionUpdateMany.mockResolvedValue({ count: 1 });
        const { handlers } = createHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: Date.now(),
            localId,
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf"
        }, callback);

        expect(mocks.sessionUpdateMany).toHaveBeenCalledWith({
            where: {
                id: "session-1",
                accountId: "account-1",
                activeInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
                active: true
            },
            data: { lastActiveAt: expect.any(Date), active: false }
        });
        expect(callback).toHaveBeenCalledWith({ result: "success", localId });
        expect(mocks.sessionUpdateMany.mock.invocationCallOrder[0])
            .toBeLessThan(callback.mock.invocationCallOrder[0]);
    });

    it("returns retryable session-end errors without a false ACK", async () => {
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        mocks.sessionUpdateMany.mockRejectedValue(new Error("database unavailable"));
        const { handlers } = createHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: Date.now(),
            localId: "54d48b15-9100-4865-985d-2e61cba5d6b9",
            sessionInstanceId: "984b458d-3e0b-456a-b849-c9d630559bd8"
        }, callback);

        expect(callback).toHaveBeenCalledWith({
            result: "error",
            code: "internal_error",
            retryable: true
        });
    });

    it("ACKs a stale session-end retry without ending a newer session", async () => {
        const localId = "eaa36e46-d645-4f8c-943a-131a6af0a477";
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        mocks.sessionUpdateMany.mockResolvedValue({ count: 0 });
        const { handlers } = createHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: 100,
            localId,
            sessionInstanceId: "6bb900b4-b4bf-4ba7-8b7b-98e399a58024"
        }, callback);

        expect(callback).toHaveBeenCalledWith({ result: "success", localId });
        expect(mocks.emitEphemeral).not.toHaveBeenCalled();
    });
});
