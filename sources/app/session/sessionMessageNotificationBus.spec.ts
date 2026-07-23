import { describe, expect, it, vi } from "vitest";
import { SessionMessageNotificationBus } from "@/app/session/sessionMessageNotificationBus";

function notification() {
    return {
        userId: "account-1",
        originSocketId: "socket-1",
        targetRuntimeConnectionLeaseId: "lease-1",
        targetLegacyRuntimeConnection: false,
        payload: {
            id: "notification-1",
            seq: 31,
            body: { t: "new-message" as const, sid: "session-1" },
            createdAt: 1,
        },
    };
}

function subscriberHarness() {
    let messageHandler: ((channel: string, payload: string) => void) | undefined;
    const subscriber = {
        subscribe: vi.fn().mockResolvedValue(1),
        unsubscribe: vi.fn().mockResolvedValue(0),
        on: vi.fn((event: string, handler: (channel: string, payload: string) => void) => {
            if (event === "message") messageHandler = handler;
            return subscriber;
        }),
        removeListener: vi.fn().mockReturnThis(),
    };
    return {
        subscriber,
        emit: (channel: string, payload: string) => messageHandler?.(channel, payload),
    };
}

describe("SessionMessageNotificationBus", () => {
    it("publishes the stable notification envelope through Redis Pub/Sub", async () => {
        const publish = vi.fn().mockResolvedValue(2);
        const harness = subscriberHarness();
        const bus = new SessionMessageNotificationBus(
            { publish } as never,
            harness.subscriber as never,
            "happy:test-notifications",
            vi.fn(),
        );
        const value = notification();

        await bus.publish(value);

        expect(publish).toHaveBeenCalledWith(
            "happy:test-notifications",
            JSON.stringify(value),
        );
    });

    it("accepts zero subscribers because cursor replay, not Redis, repairs loss", async () => {
        const publish = vi.fn().mockResolvedValue(0);
        const harness = subscriberHarness();
        const bus = new SessionMessageNotificationBus(
            { publish } as never,
            harness.subscriber as never,
            "happy:test-notifications",
            vi.fn(),
        );

        await expect(bus.publish(notification())).resolves.toBeUndefined();
    });

    it("fails a stalled publication so the database lease can be retried", async () => {
        vi.useFakeTimers();
        try {
            const harness = subscriberHarness();
            const bus = new SessionMessageNotificationBus(
                { publish: vi.fn(() => new Promise(() => {})) } as never,
                harness.subscriber as never,
                "happy:test-notifications",
                vi.fn(),
                50,
            );
            const pending = bus.publish(notification());
            const rejection = expect(pending).rejects.toThrow("publish timed out");

            await vi.advanceTimersByTimeAsync(50);
            await rejection;
        } finally {
            vi.useRealTimers();
        }
    });

    it("delivers only publications received while subscribed and shuts down cleanly", async () => {
        const harness = subscriberHarness();
        const onNotification = vi.fn();
        const bus = new SessionMessageNotificationBus(
            { publish: vi.fn() } as never,
            harness.subscriber as never,
            "happy:test-notifications",
            onNotification,
        );
        const value = notification();

        harness.emit("happy:test-notifications", JSON.stringify(value));
        expect(onNotification).not.toHaveBeenCalled();

        await bus.start();
        harness.emit("happy:other-channel", JSON.stringify(value));
        harness.emit("happy:test-notifications", JSON.stringify(value));
        await vi.waitFor(() => expect(onNotification).toHaveBeenCalledWith(value));

        await bus.stop();
        harness.emit("happy:test-notifications", JSON.stringify(value));
        expect(onNotification).toHaveBeenCalledTimes(1);
        expect(harness.subscriber.unsubscribe).toHaveBeenCalledWith("happy:test-notifications");
        expect(harness.subscriber.removeListener).toHaveBeenCalledWith("message", expect.any(Function));
    });
});
