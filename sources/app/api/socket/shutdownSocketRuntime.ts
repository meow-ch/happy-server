export interface SocketRuntimeCloser {
    close(): Promise<unknown> | unknown;
}

export interface SocketRuntimeStopper {
    stop(): Promise<unknown> | unknown;
}

export interface SocketRuntimeRedisClient {
    quit(): Promise<unknown> | unknown;
    disconnect?(): unknown;
}

export interface SocketRuntimeShutdownInput {
    io: SocketRuntimeCloser;
    rpcLifecycles: Iterable<SocketRuntimeCloser>;
    notificationDispatcher: SocketRuntimeStopper;
    notificationBus: SocketRuntimeStopper;
    redisClients: Iterable<SocketRuntimeRedisClient>;
    phaseTimeoutMs?: number;
    redisQuitTimeoutMs?: number;
}

const DEFAULT_PHASE_TIMEOUT_MS = 6_000;
const DEFAULT_REDIS_QUIT_TIMEOUT_MS = 2_000;

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

async function runBounded(
    label: string,
    operation: () => Promise<unknown> | unknown,
    timeoutMs: number,
    failures: string[],
): Promise<boolean> {
    let timer: NodeJS.Timeout | undefined;
    try {
        await Promise.race([
            Promise.resolve().then(operation),
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
            }),
        ]);
        return true;
    } catch (error) {
        failures.push(`${label}: ${errorMessage(error)}`);
        return false;
    } finally {
        if (timer) clearTimeout(timer);
    }
}

/**
 * Shut down in an order that never leaves live sockets behind a stopped bus.
 * Every external wait is bounded; RPC TTLs and forced Redis disconnects are
 * the fallback when Redis is unavailable during process termination.
 */
export async function shutdownSocketRuntime(input: SocketRuntimeShutdownInput): Promise<void> {
    const failures: string[] = [];
    const phaseTimeoutMs = input.phaseTimeoutMs ?? DEFAULT_PHASE_TIMEOUT_MS;
    const redisQuitTimeoutMs = input.redisQuitTimeoutMs ?? DEFAULT_REDIS_QUIT_TIMEOUT_MS;

    await runBounded("Socket.IO close", () => input.io.close(), phaseTimeoutMs, failures);
    await runBounded(
        "RPC registration cleanup",
        () => Promise.all([...input.rpcLifecycles].map((lifecycle) => lifecycle.close())),
        phaseTimeoutMs,
        failures,
    );
    await runBounded(
        "notification dispatcher stop",
        () => input.notificationDispatcher.stop(),
        phaseTimeoutMs,
        failures,
    );
    await runBounded(
        "notification bus stop",
        () => input.notificationBus.stop(),
        phaseTimeoutMs,
        failures,
    );
    await Promise.all([...input.redisClients].map(async (client, index) => {
        const quitCleanly = await runBounded(
            `Redis client ${index + 1} quit`,
            () => client.quit(),
            redisQuitTimeoutMs,
            failures,
        );
        if (!quitCleanly) {
            try {
                client.disconnect?.();
            } catch (error) {
                failures.push(`Redis client ${index + 1} disconnect: ${errorMessage(error)}`);
            }
        }
    }));

    if (failures.length > 0) {
        throw new Error(`Socket runtime shutdown incomplete: ${failures.join("; ")}`);
    }
}
