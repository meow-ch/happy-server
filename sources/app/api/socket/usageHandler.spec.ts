import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    sessionFindFirst: vi.fn(),
    usageUpsert: vi.fn(),
    transactionUsageUpsert: vi.fn(),
    runWithRuntimeConnectionOwnerLock: vi.fn(),
    rejectRuntimeConnection: vi.fn(),
    emitEphemeral: vi.fn(),
    buildUsageEphemeral: vi.fn(() => ({ type: "usage" })),
    log: vi.fn(),
}));

vi.mock("@/storage/db", () => ({
    db: {
        session: { findFirst: mocks.sessionFindFirst },
        usageReport: { upsert: mocks.usageUpsert },
    },
}));
vi.mock("@/app/presence/runtimeConnectionLease", () => ({
    runWithRuntimeConnectionOwnerLock: mocks.runWithRuntimeConnectionOwnerLock,
}));
vi.mock("@/app/api/socket/runtimeConnectionGuard", () => ({
    rejectRuntimeConnection: mocks.rejectRuntimeConnection,
}));
vi.mock("@/app/events/eventRouter", () => ({
    buildUsageEphemeral: mocks.buildUsageEphemeral,
    eventRouter: { emitEphemeral: mocks.emitEphemeral },
}));
vi.mock("@/utils/log", () => ({ log: mocks.log }));

import { usageHandler } from "@/app/api/socket/usageHandler";

const report = {
    id: "usage-1",
    createdAt: new Date("2026-07-22T10:00:00.000Z"),
    updatedAt: new Date("2026-07-22T10:00:01.000Z"),
};

function harness(connectionOverride: Record<string, unknown> = {}) {
    const handlers = new Map<string, (...args: any[]) => any>();
    const socket = {
        id: "socket-1",
        on: vi.fn((event: string, handler: (...args: any[]) => any) => handlers.set(event, handler)),
        disconnect: vi.fn(),
    };
    const connection = {
        connectionType: "session-scoped",
        socket,
        userId: "account-1",
        sessionId: "session-1",
        sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
        runtimeConnectionLeaseId: "lease-1",
        replayOnly: false,
        ...connectionOverride,
    };
    usageHandler("account-1", socket as never, connection as never);
    return { handlers, socket, connection };
}

const validReport = {
    key: "turn-1",
    sessionId: "session-1",
    tokens: { total: 42 },
    cost: { total: 0.01 },
};

describe("usage report runtime fencing", () => {
    beforeEach(() => {
        for (const mock of Object.values(mocks)) mock.mockReset();
        mocks.buildUsageEphemeral.mockReturnValue({ type: "usage" });
        mocks.usageUpsert.mockResolvedValue(report);
        mocks.transactionUsageUpsert.mockResolvedValue(report);
        mocks.runWithRuntimeConnectionOwnerLock.mockImplementation(async (_owner, operation) => {
            await operation({ usageReport: { upsert: mocks.transactionUsageUpsert } });
            return "completed";
        });
    });

    it("rejects a runtime report for another session in the same account", async () => {
        const subject = harness();
        const callback = vi.fn();

        await subject.handlers.get("usage-report")?.({
            ...validReport,
            sessionId: "session-2",
        }, callback);

        expect(mocks.rejectRuntimeConnection).toHaveBeenCalledWith(
            subject.connection,
            "usage report targeted a different session",
        );
        expect(mocks.runWithRuntimeConnectionOwnerLock).not.toHaveBeenCalled();
        expect(mocks.usageUpsert).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({ success: false, error: "Invalid sessionId" });
    });

    it("requires a session id from session-scoped runtimes", async () => {
        const subject = harness();
        const callback = vi.fn();

        await subject.handlers.get("usage-report")?.({
            ...validReport,
            sessionId: undefined,
        }, callback);

        expect(mocks.rejectRuntimeConnection).toHaveBeenCalled();
        expect(mocks.runWithRuntimeConnectionOwnerLock).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({ success: false, error: "Invalid sessionId" });
    });

    it("rechecks exact ownership under the claim lock before writing", async () => {
        mocks.runWithRuntimeConnectionOwnerLock.mockResolvedValue("not_owner");
        const subject = harness();
        const callback = vi.fn();

        await subject.handlers.get("usage-report")?.(validReport, callback);

        expect(mocks.runWithRuntimeConnectionOwnerLock).toHaveBeenCalledWith({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
            leaseId: "lease-1",
        }, expect.any(Function));
        expect(mocks.transactionUsageUpsert).not.toHaveBeenCalled();
        expect(mocks.emitEphemeral).not.toHaveBeenCalled();
        expect(mocks.rejectRuntimeConnection).toHaveBeenCalledWith(subject.connection, "usage report lease lost");
        expect(callback).toHaveBeenCalledWith({ success: false, error: "Runtime connection is stale" });
    });

    it("returns a retryable not-started result on owner-lock contention without disconnecting", async () => {
        mocks.runWithRuntimeConnectionOwnerLock.mockResolvedValue("busy");
        const subject = harness();
        const callback = vi.fn();

        await subject.handlers.get("usage-report")?.(validReport, callback);

        expect(mocks.transactionUsageUpsert).not.toHaveBeenCalled();
        expect(mocks.emitEphemeral).not.toHaveBeenCalled();
        expect(mocks.rejectRuntimeConnection).not.toHaveBeenCalled();
        expect(callback).toHaveBeenCalledWith({
            success: false,
            outcome: "not_started",
            retryable: true,
            error: "Runtime connection is busy",
        });
    });

    it("writes and emits while the exact runtime lease lock is held", async () => {
        const subject = harness();
        const callback = vi.fn();

        await subject.handlers.get("usage-report")?.(validReport, callback);

        expect(mocks.transactionUsageUpsert).toHaveBeenCalledWith(expect.objectContaining({
            where: {
                accountId_sessionId_key: {
                    accountId: "account-1",
                    sessionId: "session-1",
                    key: "turn-1",
                },
            },
        }));
        expect(mocks.usageUpsert).not.toHaveBeenCalled();
        expect(mocks.emitEphemeral).toHaveBeenCalledWith({
            userId: "account-1",
            payload: { type: "usage" },
            recipientFilter: { type: "user-scoped-only" },
        });
        expect(callback).toHaveBeenCalledWith({
            success: true,
            reportId: "usage-1",
            createdAt: report.createdAt.getTime(),
            updatedAt: report.updatedAt.getTime(),
        });
    });

    it("keeps user-scoped session reports account-authorized without a runtime lease", async () => {
        mocks.sessionFindFirst.mockResolvedValue({ id: "session-1" });
        const subject = harness({
            connectionType: "user-scoped",
            userId: "account-1",
            sessionId: undefined,
            sessionInstanceId: undefined,
            runtimeConnectionLeaseId: undefined,
        });

        await subject.handlers.get("usage-report")?.(validReport, vi.fn());

        expect(mocks.sessionFindFirst).toHaveBeenCalledWith({
            where: { id: "session-1", accountId: "account-1" },
        });
        expect(mocks.usageUpsert).toHaveBeenCalled();
        expect(mocks.runWithRuntimeConnectionOwnerLock).not.toHaveBeenCalled();
    });
});
