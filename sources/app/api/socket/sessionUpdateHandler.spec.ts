import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    persistSessionMessage: vi.fn(),
    toSessionMessageAck: vi.fn((result: any) => ({ result: result.result })),
    log: vi.fn(),
    sessionFindUnique: vi.fn(),
    sessionUpdateMany: vi.fn(),
    emitEphemeral: vi.fn(),
    publishSessionActivity: vi.fn(),
    endRuntimeConnectionLease: vi.fn(),
    renewRuntimeConnectionLease: vi.fn(),
    renewLegacyRuntimeConnection: vi.fn(),
    isRuntimeConnectionOwner: vi.fn(),
    updateRuntimeSessionMetadata: vi.fn(),
    updateRuntimeSessionState: vi.fn(),
    newRuntimeConnectionLeaseTombstone: vi.fn(() => "lease-tombstone"),
}));

vi.mock("@/app/monitoring/metrics2", () => ({
    sessionAliveEventsCounter: { inc: vi.fn() },
    websocketEventsCounter: { inc: vi.fn() }
}));
vi.mock("@/app/presence/sessionActivityPublisher", () => ({
    sessionActivityPublisher: { publish: mocks.publishSessionActivity }
}));
vi.mock("@/app/presence/runtimeConnectionLease", () => ({
    endRuntimeConnectionLease: mocks.endRuntimeConnectionLease,
    renewRuntimeConnectionLease: mocks.renewRuntimeConnectionLease,
    renewLegacyRuntimeConnection: mocks.renewLegacyRuntimeConnection,
    isRuntimeConnectionOwner: mocks.isRuntimeConnectionOwner,
    updateRuntimeSessionMetadata: mocks.updateRuntimeSessionMetadata,
    updateRuntimeSessionState: mocks.updateRuntimeSessionState,
    newRuntimeConnectionLeaseTombstone: mocks.newRuntimeConnectionLeaseTombstone,
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

function createHarness(connectionOverride?: Record<string, unknown>) {
    const handlers = new Map<string, (...args: any[]) => any>();
    const socket = {
        id: "socket-1",
        disconnect: vi.fn(),
        on: vi.fn((event: string, handler: (...args: any[]) => any) => handlers.set(event, handler))
    };
    const dispatcher = {
        wake: vi.fn()
    };
    const connection = {
        ...(connectionOverride ?? {
            connectionType: "user-scoped",
            userId: "account-1",
        }),
        socket,
    };
    sessionUpdateHandler(
        "account-1",
        socket as never,
        connection as never,
        dispatcher as never
    );
    return { handlers, dispatcher, socket, connection };
}

function createSessionHarness() {
    return createHarness({
        connectionType: "session-scoped",
        socket: { id: "socket-1" },
        userId: "account-1",
        sessionId: "session-1",
        sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
        runtimeConnectionLeaseId: "lease-generation-1",
        replayOnly: false,
    });
}

describe("session message socket handler", () => {
    beforeEach(() => {
        mocks.persistSessionMessage.mockReset();
        mocks.toSessionMessageAck.mockClear();
        mocks.log.mockClear();
        mocks.sessionFindUnique.mockReset();
        mocks.sessionUpdateMany.mockReset();
        mocks.emitEphemeral.mockReset();
        mocks.publishSessionActivity.mockReset();
        mocks.endRuntimeConnectionLease.mockReset();
        mocks.renewRuntimeConnectionLease.mockReset();
        mocks.renewLegacyRuntimeConnection.mockReset();
        mocks.isRuntimeConnectionOwner.mockReset();
        mocks.updateRuntimeSessionMetadata.mockReset();
        mocks.updateRuntimeSessionState.mockReset();
        mocks.newRuntimeConnectionLeaseTombstone.mockClear();
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

    it("renews every heartbeat through the exact socket-generation CAS", async () => {
        mocks.renewRuntimeConnectionLease.mockResolvedValue(true);
        const receivedAt = 1_753_200_000_000;
        const now = vi.spyOn(Date, "now").mockReturnValue(receivedAt);
        const { handlers } = createSessionHarness();

        await handlers.get("session-alive")?.({
            sid: "session-1",
            time: 1,
            thinking: true
        });

        expect(mocks.renewRuntimeConnectionLease).toHaveBeenCalledWith({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
            leaseId: "lease-generation-1",
        });
        expect(mocks.publishSessionActivity).toHaveBeenCalledWith({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: receivedAt,
            thinking: true
        });
        now.mockRestore();
    });

    it("allows a legacy session heartbeat only through the lease-null CAS path", async () => {
        mocks.renewLegacyRuntimeConnection.mockResolvedValue(true);
        const receivedAt = 1_753_200_000_000;
        const now = vi.spyOn(Date, "now").mockReturnValue(receivedAt);
        const { handlers } = createHarness({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            replayOnly: false,
        });

        await handlers.get("session-alive")?.({
            sid: "session-1",
            time: 1,
            thinking: false,
        });

        expect(mocks.renewLegacyRuntimeConnection).toHaveBeenCalledWith({
            accountId: "account-1",
            sessionId: "session-1",
        });
        now.mockRestore();
    });

    it("suppresses a stale generation while the winning generation keeps renewing", async () => {
        mocks.renewRuntimeConnectionLease
            .mockResolvedValueOnce(false)
            .mockResolvedValue(true);
        const stale = createHarness({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
            runtimeConnectionLeaseId: "old-lease",
            replayOnly: false,
        });
        const winner = createHarness({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
            runtimeConnectionLeaseId: "new-lease",
            replayOnly: false,
        });

        await stale.handlers.get("session-alive")?.({ sid: "session-1", time: 1 });
        await stale.handlers.get("session-alive")?.({ sid: "session-1", time: 2 });
        await winner.handlers.get("session-alive")?.({ sid: "session-1", time: 3 });
        await winner.handlers.get("session-alive")?.({ sid: "session-1", time: 4 });

        expect(mocks.renewRuntimeConnectionLease).toHaveBeenCalledTimes(3);
        expect(stale.socket.disconnect).toHaveBeenCalledWith(true);
        expect(winner.socket.disconnect).not.toHaveBeenCalled();
        expect(mocks.publishSessionActivity).toHaveBeenCalledTimes(2);
    });

    it("does not let a user-scoped socket renew runtime presence", async () => {
        const { handlers } = createHarness();

        await handlers.get("session-alive")?.({
            sid: "session-1",
            time: Date.now(),
        });

        expect(mocks.renewRuntimeConnectionLease).not.toHaveBeenCalled();
        expect(mocks.renewLegacyRuntimeConnection).not.toHaveBeenCalled();
    });

    it("passes exact lease authorization into message persistence and rejects another sid", async () => {
        mocks.persistSessionMessage.mockResolvedValue(success("message-1", 1));
        const valid = createSessionHarness();
        await valid.handlers.get("message")?.({
            sid: "session-1",
            message: "ciphertext",
            localId: "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3",
        }, vi.fn());
        expect(mocks.persistSessionMessage).toHaveBeenCalledWith(expect.objectContaining({
            runtimeAuthorization: {
                type: "lease",
                sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
                leaseId: "lease-generation-1",
            },
        }));

        mocks.persistSessionMessage.mockClear();
        const wrongSession = createSessionHarness();
        await wrongSession.handlers.get("message")?.({
            sid: "session-2",
            message: "ciphertext",
        }, vi.fn());
        expect(mocks.persistSessionMessage).not.toHaveBeenCalled();
        expect(wrongSession.socket.disconnect).toHaveBeenCalledWith(true);
    });

    it("disconnects when the row-locked message lease fence loses a claim race", async () => {
        mocks.persistSessionMessage.mockResolvedValue({
            result: "error",
            code: "runtime_connection_stale",
            retryable: true,
        });
        const harness = createSessionHarness();
        await harness.handlers.get("message")?.({
            sid: "session-1",
            message: "ciphertext",
        }, vi.fn());
        expect(harness.socket.disconnect).toHaveBeenCalledWith(true);
        expect(harness.dispatcher.wake).not.toHaveBeenCalled();
    });

    it("acknowledges session-end only after the inactive state is persisted", async () => {
        const localId = "16cc3995-c10f-43c0-8dbd-95ef41f924a7";
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        const endedAt = new Date("2026-07-22T18:00:00.000Z");
        mocks.endRuntimeConnectionLease.mockResolvedValue(endedAt);
        const { handlers } = createSessionHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: Date.now(),
            localId,
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf"
        }, callback);

        expect(mocks.endRuntimeConnectionLease).toHaveBeenCalledWith({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf",
        });
        expect(callback).toHaveBeenCalledWith({ result: "success", localId });
        expect(mocks.endRuntimeConnectionLease.mock.invocationCallOrder[0])
            .toBeLessThan(callback.mock.invocationCallOrder[0]);
    });

    it("returns retryable session-end errors without a false ACK", async () => {
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        mocks.endRuntimeConnectionLease.mockRejectedValue(new Error("database unavailable"));
        const { handlers } = createSessionHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: Date.now(),
            localId: "54d48b15-9100-4865-985d-2e61cba5d6b9",
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf"
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
        mocks.endRuntimeConnectionLease.mockResolvedValue(null);
        const { handlers } = createSessionHarness();
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: 100,
            localId,
            sessionInstanceId: "8c46b5ad-4155-47ed-a470-d21c7be49baf"
        }, callback);

        expect(callback).toHaveBeenCalledWith({ result: "success", localId });
        expect(mocks.publishSessionActivity).not.toHaveBeenCalled();
    });

    it("treats a duplicate-only no-ID replay end as an idempotent no-op", async () => {
        const localId = "eaa36e46-d645-4f8c-943a-131a6af0a477";
        mocks.sessionFindUnique.mockResolvedValue({ id: "session-1" });
        mocks.sessionUpdateMany.mockResolvedValue({ count: 0 });
        const { handlers } = createHarness({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            replayOnly: true,
        });
        const callback = vi.fn();

        await handlers.get("session-end")?.({
            sid: "session-1",
            time: Date.now(),
        }, callback);

        expect(mocks.sessionUpdateMany).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({ result: "success", localId: null });
        expect(mocks.publishSessionActivity).not.toHaveBeenCalled();
    });
});
