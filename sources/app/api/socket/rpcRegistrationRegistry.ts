import { randomUUID, createHash } from "node:crypto";
import { redis } from "@/storage/redis";
import { log } from "@/utils/log";
import type { Redis } from "ioredis";

const DEFAULT_REGISTRATION_TTL_MS = 60_000;
const DEFAULT_REFRESH_INTERVAL_MS = 20_000;
const DEFAULT_MAX_REGISTRATIONS_PER_SOCKET = 256;
const REGISTRY_KEY_PREFIX = "happy:rpc-registration:v1";
const REGISTRATION_GENERATION_PATTERN = /^\d{16}:\d{8}:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const REFRESH_IF_CURRENT_SCRIPT = `
local current = redis.call("GET", KEYS[1])
if current == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
if not current then
    local reclaimed = redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
    if reclaimed then
        return 1
    end
    current = redis.call("GET", KEYS[1])
end

local current_ok, current_target = pcall(cjson.decode, current)
local candidate_ok, candidate_target = pcall(cjson.decode, ARGV[1])
if current_ok and candidate_ok
    and type(current_target) == "table"
    and type(candidate_target) == "table"
    and type(current_target["generation"]) == "string"
    and type(candidate_target["generation"]) == "string"
    and current_target["generation"] < candidate_target["generation"] then
    redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
    return 1
end
return 0
`;

const DELETE_IF_CURRENT_SCRIPT = `
local current = redis.call("GET", KEYS[1])
if current == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`;

type RedisCommands = Pick<Redis, "get" | "set" | "eval">;

let lastRegistrationTimestampMs = 0;
let lastRegistrationSequence = 0;

export type RpcRegistrationTarget = {
    socketId: string;
    generation: string;
};

export type RpcRegistration = RpcRegistrationTarget & {
    key: string;
};

function encodeTarget(target: RpcRegistrationTarget): string {
    return JSON.stringify({
        socketId: target.socketId,
        generation: target.generation,
    });
}

function createGeneration(): string {
    // A small hybrid logical clock preserves local registration order when
    // several registrations share a millisecond or the wall clock steps back.
    // UUID then supplies a deterministic cross-server tie-break without adding
    // a third Redis value field.
    const observedAtMs = Date.now();
    if (observedAtMs > lastRegistrationTimestampMs) {
        lastRegistrationTimestampMs = observedAtMs;
        lastRegistrationSequence = 0;
    } else {
        lastRegistrationSequence += 1;
        if (lastRegistrationSequence > 99_999_999) {
            lastRegistrationTimestampMs += 1;
            lastRegistrationSequence = 0;
        }
    }
    return `${lastRegistrationTimestampMs.toString().padStart(16, "0")}:${lastRegistrationSequence.toString().padStart(8, "0")}:${randomUUID()}`;
}

function parseTarget(value: string | null): RpcRegistrationTarget | null {
    if (value === null) return null;

    try {
        const parsed = JSON.parse(value) as Record<string, unknown>;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
        if (typeof parsed.socketId !== "string" || parsed.socketId.length === 0 || parsed.socketId.length > 256) return null;
        if (typeof parsed.generation !== "string" || !REGISTRATION_GENERATION_PATTERN.test(parsed.generation)) return null;
        return {
            socketId: parsed.socketId,
            generation: parsed.generation,
        };
    } catch {
        return null;
    }
}

/** Hash account and method text into a bounded Redis key without retaining either value. */
export function getRpcRegistrationKey(userId: string, method: string): string {
    const digest = createHash("sha256")
        .update(userId)
        .update("\0")
        .update(method)
        .digest("base64url");
    return `${REGISTRY_KEY_PREFIX}:${digest}`;
}

export class RedisRpcRegistrationRegistry {
    constructor(
        private readonly client: RedisCommands,
        private readonly ttlMs = DEFAULT_REGISTRATION_TTL_MS,
    ) {
        if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
            throw new Error("RPC registration TTL must be a positive integer");
        }
    }

    async register(userId: string, method: string, socketId: string): Promise<RpcRegistration> {
        const registration: RpcRegistration = {
            key: getRpcRegistrationKey(userId, method),
            socketId,
            generation: createGeneration(),
        };
        await this.client.set(
            registration.key,
            encodeTarget(registration),
            "PX",
            this.ttlMs,
        );
        return registration;
    }

    async resolve(userId: string, method: string): Promise<RpcRegistrationTarget | null> {
        return parseTarget(await this.client.get(getRpcRegistrationKey(userId, method)));
    }

    async refresh(registration: RpcRegistration): Promise<boolean> {
        const result = await this.client.eval(
            REFRESH_IF_CURRENT_SCRIPT,
            1,
            registration.key,
            encodeTarget(registration),
            this.ttlMs,
        );
        return Number(result) === 1;
    }

    async unregister(registration: RpcRegistration): Promise<boolean> {
        const result = await this.client.eval(
            DELETE_IF_CURRENT_SCRIPT,
            1,
            registration.key,
            encodeTarget(registration),
        );
        return Number(result) === 1;
    }
}

export const rpcRegistrationRegistry = new RedisRpcRegistrationRegistry(redis);

/**
 * Per-socket lifecycle. Its generation-checked refresh converges Redis-loss
 * recovery to the freshest live registration and cleanup cannot delete a newer
 * socket's registration for the same method.
 */
export class RpcRegistrationLifecycle {
    private readonly registrations = new Map<string, RpcRegistration>();
    private operationTail: Promise<void> = Promise.resolve();
    private refreshTimer: ReturnType<typeof setInterval> | undefined;
    private closing = false;
    private closePromise: Promise<void> | undefined;

    constructor(
        private readonly userId: string,
        private readonly socketId: string,
        private readonly registry: RedisRpcRegistrationRegistry = rpcRegistrationRegistry,
        private readonly refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS,
        private readonly maxRegistrations = DEFAULT_MAX_REGISTRATIONS_PER_SOCKET,
    ) {
        if (!Number.isSafeInteger(refreshIntervalMs) || refreshIntervalMs <= 0) {
            throw new Error("RPC registration refresh interval must be a positive integer");
        }
        if (!Number.isSafeInteger(maxRegistrations) || maxRegistrations <= 0) {
            throw new Error("RPC registration limit must be a positive integer");
        }
    }

    register(method: string): Promise<void> {
        return this.runExclusive(async () => {
            if (this.closing) throw new Error("RPC registration lifecycle is closed");
            if (!this.registrations.has(method) && this.registrations.size >= this.maxRegistrations) {
                throw new Error("RPC registration limit reached");
            }
            const registration = await this.registry.register(this.userId, method, this.socketId);
            this.registrations.set(method, registration);
            this.ensureRefreshTimer();
        });
    }

    unregister(method: string): Promise<void> {
        return this.runExclusive(async () => {
            const registration = this.registrations.get(method);
            if (!registration) return;
            this.registrations.delete(method);
            this.stopRefreshTimerIfIdle();
            await this.registry.unregister(registration);
        });
    }

    resolve(method: string): Promise<RpcRegistrationTarget | null> {
        return this.registry.resolve(this.userId, method);
    }

    /** Refresh immediately; exported primarily for lifecycle wiring and tests. */
    refresh(): Promise<void> {
        return this.runExclusive(async () => {
            if (this.closing) return;
            const registrations = [...this.registrations.entries()];
            const results = await Promise.allSettled(
                registrations.map(([, registration]) => this.registry.refresh(registration)),
            );

            results.forEach((result, index) => {
                const [method, registration] = registrations[index];
                if (result.status === "rejected") {
                    log(
                        { module: "websocket-rpc", level: "error" },
                        `Failed to refresh RPC registration for socket ${this.socketId}: ${result.reason}`,
                    );
                    return;
                }
                if (!result.value && this.registrations.get(method)?.generation === registration.generation) {
                    this.registrations.delete(method);
                }
            });
            this.stopRefreshTimerIfIdle();
        });
    }

    /** Compare-delete every registration owned by this socket and stop heartbeats. */
    close(): Promise<void> {
        if (this.closePromise) return this.closePromise;
        this.closing = true;
        this.stopRefreshTimer();
        this.closePromise = this.runExclusive(async () => {
            const registrations = [...this.registrations.values()];
            this.registrations.clear();
            const results = await Promise.allSettled(
                registrations.map((registration) => this.registry.unregister(registration)),
            );
            for (const result of results) {
                if (result.status === "rejected") {
                    log(
                        { module: "websocket-rpc", level: "error" },
                        `Failed to remove RPC registration for socket ${this.socketId}: ${result.reason}`,
                    );
                }
            }
        });
        return this.closePromise;
    }

    private ensureRefreshTimer(): void {
        if (this.refreshTimer || this.closing) return;
        this.refreshTimer = setInterval(() => {
            void this.refresh();
        }, this.refreshIntervalMs);
        this.refreshTimer.unref?.();
    }

    private stopRefreshTimerIfIdle(): void {
        if (this.registrations.size === 0) this.stopRefreshTimer();
    }

    private stopRefreshTimer(): void {
        if (!this.refreshTimer) return;
        clearInterval(this.refreshTimer);
        this.refreshTimer = undefined;
    }

    private runExclusive<T>(operation: () => Promise<T>): Promise<T> {
        const result = this.operationTail.then(operation, operation);
        this.operationTail = result.then(
            () => undefined,
            () => undefined,
        );
        return result;
    }
}

export function createRpcRegistrationLifecycle(
    userId: string,
    socketId: string,
    registry: RedisRpcRegistrationRegistry = rpcRegistrationRegistry,
    refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS,
): RpcRegistrationLifecycle {
    return new RpcRegistrationLifecycle(userId, socketId, registry, refreshIntervalMs);
}
