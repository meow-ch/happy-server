import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    isRuntimeConnectionOwner: vi.fn(),
    log: vi.fn(),
}));

vi.mock("@/app/presence/runtimeConnectionLease", () => ({
    isRuntimeConnectionOwner: mocks.isRuntimeConnectionOwner,
}));
vi.mock("@/utils/log", () => ({ log: mocks.log }));

import { installRuntimeConnectionPacketFence } from "@/app/api/socket/runtimeConnectionGuard";

function harness(overrides: Record<string, unknown> = {}) {
    let middleware: ((packet: unknown[], next: (error?: Error) => void) => Promise<void>) | undefined;
    const socket = {
        id: "socket-1",
        use: vi.fn((handler) => { middleware = handler; }),
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
        ...overrides,
    };
    installRuntimeConnectionPacketFence(connection as never);
    return { socket, connection, dispatch: (event: string) => {
        const next = vi.fn();
        const pending = middleware?.([event], next);
        return { next, pending };
    } };
}

describe("runtime connection packet fence", () => {
    beforeEach(() => {
        mocks.isRuntimeConnectionOwner.mockReset();
        mocks.log.mockReset();
    });

    it("allows a current runtime's RPC registration only after exact ownership", async () => {
        mocks.isRuntimeConnectionOwner.mockResolvedValue(true);
        const subject = harness();
        const call = subject.dispatch("rpc-register");
        await call.pending;
        expect(mocks.isRuntimeConnectionOwner).toHaveBeenCalledWith({
            accountId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "90b85ebd-6bb8-41bb-aa2d-681765e24f0d",
            leaseId: "lease-1",
        });
        expect(call.next).toHaveBeenCalledWith();
        expect(subject.socket.disconnect).not.toHaveBeenCalled();
    });

    it("disconnects a stale generation before its handler runs", async () => {
        mocks.isRuntimeConnectionOwner.mockResolvedValue(false);
        const subject = harness();
        const call = subject.dispatch("update-state");
        await call.pending;
        expect(call.next).toHaveBeenCalledWith(expect.any(Error));
        expect(subject.socket.disconnect).toHaveBeenCalledWith(true);
    });

    it("rejects cross-scope mutations and session-originated RPC calls without a DB precheck", async () => {
        for (const event of ["artifact-delete", "machine-update-state", "access-key-get", "rpc-call"]) {
            const subject = harness();
            const call = subject.dispatch(event);
            await call.pending;
            expect(call.next).toHaveBeenCalledWith(expect.any(Error));
            expect(subject.socket.disconnect).toHaveBeenCalledWith(true);
        }
        expect(mocks.isRuntimeConnectionOwner).not.toHaveBeenCalled();
    });

    it("keeps replay transports write-only and out of live operations", async () => {
        const allowed = harness({ replayOnly: true });
        const message = allowed.dispatch("message");
        await message.pending;
        expect(message.next).toHaveBeenCalledWith();

        const blocked = harness({ replayOnly: true });
        const registration = blocked.dispatch("rpc-register");
        await registration.pending;
        expect(registration.next).toHaveBeenCalledWith(expect.any(Error));
        expect(blocked.socket.disconnect).toHaveBeenCalledWith(true);
    });
});
