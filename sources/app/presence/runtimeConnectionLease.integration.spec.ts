import { expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { db } from "@/storage/db";
import {
    expireRuntimeConnectionLease,
    runWithRuntimeConnectionOwnerLock,
} from "@/app/presence/runtimeConnectionLease";

const integrationIt = process.env.HAPPY_RUNTIME_LEASE_INTEGRATION === "1" ? it : it.skip;

integrationIt("fails fast under real PostgreSQL owner-lock contention", async () => {
    const accountId = `lease-test-${randomUUID()}`;
    const sessionId = `lease-test-${randomUUID()}`;
    const instanceId = randomUUID();
    const leaseId = randomUUID();
    const owner = {
        accountId,
        sessionId,
        sessionInstanceId: instanceId,
        leaseId,
    };
    let releaseFirst: (() => void) | undefined;
    let first: Promise<unknown> | undefined;

    try {
        await db.account.create({
            data: { id: accountId, publicKey: `pk-${randomUUID()}` },
        });
        await db.session.create({
            data: {
                id: sessionId,
                accountId,
                tag: `tag-${randomUUID()}`,
                metadata: "encrypted",
                active: true,
                activeInstanceId: instanceId,
                runtimeConnectionLeaseId: leaseId,
                runtimeConnectionLeaseInstanceId: instanceId,
                runtimeConnectionLeaseExpiresAt: new Date(Date.now() + 10 * 60_000),
            },
        });

        let signalEntered!: () => void;
        const entered = new Promise<void>(resolve => { signalEntered = resolve; });
        first = runWithRuntimeConnectionOwnerLock(owner, async () => {
            signalEntered();
            await new Promise<void>(resolve => { releaseFirst = resolve; });
        });
        await entered;

        let secondExecuted = false;
        const startedAt = Date.now();
        let contentionTimer: ReturnType<typeof setTimeout>;
        const timeout = new Promise<never>((_resolve, reject) => {
            contentionTimer = setTimeout(
                () => reject(new Error("contending owner lock blocked")),
                2_000,
            );
        });
        const second = await Promise.race([
            runWithRuntimeConnectionOwnerLock(owner, async () => {
                secondExecuted = true;
            }),
            timeout,
        ]);
        clearTimeout(contentionTimer!);

        expect(second).toBe("busy");
        expect(secondExecuted).toBe(false);
        expect(Date.now() - startedAt).toBeLessThan(2_000);

        releaseFirst?.();
        await expect(first).resolves.toBe("completed");
        first = undefined;

        let signalRevocationEntered!: () => void;
        const revocationEntered = new Promise<void>(resolve => {
            signalRevocationEntered = resolve;
        });
        first = runWithRuntimeConnectionOwnerLock(owner, async () => {
            signalRevocationEntered();
            await new Promise<void>(resolve => { releaseFirst = resolve; });
        });
        await revocationEntered;
        let expirationFinished = false;
        const expiration = expireRuntimeConnectionLease(owner)
            .then(result => {
                expirationFinished = true;
                return result;
            });
        await new Promise(resolve => setTimeout(resolve, 50));
        expect(expirationFinished).toBe(false);
        releaseFirst?.();
        await expect(first).resolves.toBe("completed");
        first = undefined;
        await expect(expiration).resolves.toBe(true);

        let staleExecuted = false;
        await expect(runWithRuntimeConnectionOwnerLock(owner, async () => {
            staleExecuted = true;
        })).resolves.toBe("not_owner");
        expect(staleExecuted).toBe(false);
    } finally {
        releaseFirst?.();
        await first?.catch(() => undefined);
        await db.session.deleteMany({ where: { id: sessionId } });
        await db.account.deleteMany({ where: { id: accountId } });
    }
}, 15_000);
