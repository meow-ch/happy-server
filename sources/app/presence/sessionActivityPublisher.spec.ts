import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
    buildSessionActivityEphemeral: vi.fn((sessionId, active, activeAt, thinking) => ({
        type: "activity",
        id: sessionId,
        active,
        activeAt,
        thinking,
    })),
    emitEphemeral: vi.fn(),
    publicationCounter: { inc: vi.fn() },
}));

vi.mock("@/app/events/eventRouter", () => ({
    buildSessionActivityEphemeral: mocks.buildSessionActivityEphemeral,
    eventRouter: { emitEphemeral: mocks.emitEphemeral },
}));
vi.mock("@/app/monitoring/metrics2", () => ({
    sessionActivityPublicationsCounter: mocks.publicationCounter,
}));

import { SessionActivityPublisher } from "@/app/presence/sessionActivityPublisher";

describe("SessionActivityPublisher", () => {
    let now: number;
    let publisher: SessionActivityPublisher;

    beforeEach(() => {
        now = 1_000;
        mocks.buildSessionActivityEphemeral.mockClear();
        mocks.emitEphemeral.mockClear();
        mocks.publicationCounter.inc.mockClear();
        publisher = new SessionActivityPublisher({
            refreshIntervalMs: 30_000,
            staleEntryTtlMs: 120_000,
            now: () => now,
        });
    });

    it("coalesces unchanged heartbeats but publishes state changes and bounded refreshes", () => {
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 1_000,
            thinking: false,
        })).toBe(true);

        now += 2_000;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 3_000,
            thinking: false,
        })).toBe(false);

        now += 2_000;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 5_000,
            thinking: true,
        })).toBe(true);

        now += 29_999;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 34_999,
            thinking: true,
        })).toBe(false);

        now += 1;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 35_000,
            thinking: true,
        })).toBe(true);

        expect(mocks.emitEphemeral).toHaveBeenCalledTimes(3);
        expect(mocks.emitEphemeral).toHaveBeenLastCalledWith({
            userId: "account-1",
            payload: {
                type: "activity",
                id: "session-1",
                active: true,
                activeAt: 35_000,
                thinking: true,
            },
            recipientFilter: { type: "user-scoped-only" },
        });
    });

    it("publishes inactive and reactivated transitions immediately", () => {
        publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 1_000,
            thinking: false,
        });

        now += 1;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: false,
            activeAt: 1_001,
            thinking: false,
        })).toBe(true);

        now += 1;
        expect(publisher.publish({
            userId: "account-1",
            sessionId: "session-1",
            active: true,
            activeAt: 1_002,
            thinking: false,
        })).toBe(true);

        expect(mocks.emitEphemeral).toHaveBeenCalledTimes(3);
    });

    it("coalesces independently per account and session", () => {
        const heartbeat = {
            sessionId: "session-1",
            active: true,
            activeAt: 1_000,
            thinking: false,
        };

        expect(publisher.publish({ ...heartbeat, userId: "account-1" })).toBe(true);
        expect(publisher.publish({ ...heartbeat, userId: "account-2" })).toBe(true);
        expect(publisher.publish({ ...heartbeat, userId: "account-1", sessionId: "session-2" })).toBe(true);
        expect(mocks.emitEphemeral).toHaveBeenCalledTimes(3);
    });
});
