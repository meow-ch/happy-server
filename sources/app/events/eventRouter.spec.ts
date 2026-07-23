import { describe, expect, it } from "vitest";
import { vi } from "vitest";

vi.mock("@/storage/files", () => ({
    getPublicUrl: (path: string) => path
}));

import { eventRouter, getConnectionRooms, getRecipientRooms } from "@/app/events/eventRouter";

describe("Socket.IO event rooms", () => {
    it("joins every connection to an account room and its scoped room", () => {
        expect(getConnectionRooms({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1"
        })).toEqual([
            "account:account-1:all",
            "account:account-1:session:session-1"
        ]);
    });

    it("isolates runtime generations in lease rooms and gives replay transports no live room", () => {
        expect(getConnectionRooms({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "instance-1",
            runtimeConnectionLeaseId: "lease-new",
        })).toEqual([
            "account:account-1:all",
            "account:account-1:session:session-1:runtime-lease:lease-new",
        ]);
        expect(getConnectionRooms({
            connectionType: "session-scoped",
            userId: "account-1",
            sessionId: "session-1",
            sessionInstanceId: "instance-1",
            runtimeConnectionLeaseId: "lease-old",
            replayOnly: true,
        })).toEqual([]);
    });

    it("routes session updates to user observers and the matching session across nodes", () => {
        expect(getRecipientRooms("account-1", {
            type: "all-interested-in-session",
            sessionId: "session-1"
        })).toEqual([
            "account:account-1:user",
            "account:account-1:session:session-1"
        ]);
    });

    it("routes machine updates only to user observers and the selected machine", () => {
        expect(getRecipientRooms("account-1", {
            type: "machine-scoped-only",
            machineId: "machine-1"
        })).toEqual([
            "account:account-1:user",
            "account:account-1:machine:machine-1"
        ]);
    });

    it("delivers durable notifications locally without republishing them to the cluster", () => {
        const emit = vi.fn();
        const except = vi.fn(() => ({ emit }));
        const to = vi.fn(() => ({ except, emit }));
        eventRouter.setServer({ local: { to } } as never);

        const payload = {
            id: "notification-1",
            seq: 31,
            body: { t: "new-message" as const, sid: "session-1" },
            createdAt: 1
        };
        eventRouter.emitDurableSessionMessageLocal({
            userId: "account-1",
            originSocketId: "producer-socket",
            targetRuntimeConnectionLeaseId: "lease-1",
            targetLegacyRuntimeConnection: false,
            payload
        });

        expect(to).toHaveBeenCalledWith([
            "account:account-1:user",
            "account:account-1:session:session-1:runtime-lease:lease-1"
        ]);
        expect(except).toHaveBeenCalledWith("producer-socket");
        expect(emit).toHaveBeenCalledWith("update", payload);
    });
});
