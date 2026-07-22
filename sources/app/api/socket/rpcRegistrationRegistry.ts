import { randomUUID, createHash } from "node:crypto";
import { redis } from "@/storage/redis";
import { log } from "@/utils/log";
import type { Redis } from "ioredis";
import {
    rpcRegistrationRefreshBatchSizeHistogram,
    rpcRegistrationRefreshCounter,
} from "@/app/monitoring/metrics2";

const DEFAULT_REGISTRATION_TTL_MS = 60_000;
const DEFAULT_REFRESH_INTERVAL_MS = 20_000;
const DEFAULT_MAX_REGISTRATIONS_PER_SOCKET = 256;
const REGISTRY_KEY_PREFIX = "happy:rpc-registration:v1";
const REGISTRATION_GENERATION_PATTERN = /^\d{16}:\d{8}:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const REFRESH_CURRENT_BATCH_SCRIPT = `
local ttl = ARGV[#ARGV]
local results = {}
for index, key in ipairs(KEYS) do
    local candidate = ARGV[index]
    local current = redis.call("GET", key)
    if current == candidate then
        results[index] = redis.call("PEXPIRE", key, ttl)
    elseif not current then
        local reclaimed = redis.call("SET", key, candidate, "PX", ttl, "NX")
        if reclaimed then
            results[index] = 1
        else
            current = redis.call("GET", key)
        end
    end

    if results[index] == nil then
        local current_ok, current_target = pcall(cjson.decode, current)
        local candidate_ok, candidate_target = pcall(cjson.decode, candidate)
        if current_ok and candidate_ok
            and type(current_target) == "table"
            and type(candidate_target) == "table"
            and type(current_target["generation"]) == "string"
            and type(candidate_target["generation"]) == "string"
            and current_target["generation"] < candidate_target["generation"] then
            redis.call("SET", key, candidate, "PX", ttl)
            results[index] = 1
        else
            results[index] = 0
        end
    end
end
return results
`;

const DELETE_CURRENT_BATCH_SCRIPT = `
local results = {}
for index, key in ipairs(KEYS) do
    local current = redis.call("GET", key)
    if current == ARGV[index] then
        results[index] = redis.call("DEL", key)
    else
        results[index] = 0
    end
end
return results
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
        return (await this.refreshMany([registration]))[0] ?? false;
    }

    async refreshMany(registrations: RpcRegistration[]): Promise<boolean[]> {
        if (registrations.length === 0) return [];
        const result = await this.client.eval(
            REFRESH_CURRENT_BATCH_SCRIPT,
            registrations.length,
            ...registrations.map(registration => registration.key),
            ...registrations.map(encodeTarget),
            this.ttlMs,
        );
        if (!Array.isArray(result) || result.length !== registrations.length) {
            throw new Error("Invalid batched RPC registration refresh response");
        }
        return registrations.map((_registration, index) => Number(result[index]) === 1);
    }

    async unregister(registration: RpcRegistration): Promise<boolean> {
        return (await this.unregisterMany([registration]))[0] ?? false;
    }

    async unregisterMany(registrations: RpcRegistration[]): Promise<boolean[]> {
        if (registrations.length === 0) return [];
        const result = await this.client.eval(
            DELETE_CURRENT_BATCH_SCRIPT,
            registrations.length,
            ...registrations.map(registration => registration.key),
            ...registrations.map(encodeTarget),
        );
        if (!Array.isArray(result) || result.length !== registrations.length) {
            throw new Error("Invalid batched RPC registration removal response");
        }
        return registrations.map((_registration, index) => Number(result[index]) === 1);
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
    private refreshPromise: Promise<void> | undefined;
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
        if (this.refreshPromise) return this.refreshPromise;
        const refreshPromise = this.runExclusive(async () => {
            if (this.closing) return;
            const registrations = [...this.registrations.entries()];
            if (registrations.length === 0) return;
            rpcRegistrationRefreshBatchSizeHistogram.observe(registrations.length);
            try {
                const results = await this.registry.refreshMany(
                    registrations.map(([, registration]) => registration),
                );
                rpcRegistrationRefreshCounter.inc({ result: "batch_success" });
                results.forEach((isCurrent, index) => {
                    const [method, registration] = registrations[index];
                    if (!isCurrent && this.registrations.get(method)?.generation === registration.generation) {
                        this.registrations.delete(method);
                        rpcRegistrationRefreshCounter.inc({ result: "registration_lost" });
                    } else if (isCurrent) {
                        rpcRegistrationRefreshCounter.inc({ result: "registration_refreshed" });
                    }
                });
            } catch (error) {
                rpcRegistrationRefreshCounter.inc({ result: "batch_failure" });
                log(
                    { module: "websocket-rpc", level: "error" },
                    `Failed to refresh RPC registrations for socket ${this.socketId}: ${error}`,
                );
            } finally {
                this.stopRefreshTimerIfIdle();
            }
        }).finally(() => {
            this.refreshPromise = undefined;
        });
        this.refreshPromise = refreshPromise;
        return refreshPromise;
    }

    /** Compare-delete every registration owned by this socket and stop heartbeats. */
    close(): Promise<void> {
        if (this.closePromise) return this.closePromise;
        this.closing = true;
        this.stopRefreshTimer();
        this.closePromise = this.runExclusive(async () => {
            const registrations = [...this.registrations.values()];
            this.registrations.clear();
            try {
                await this.registry.unregisterMany(registrations);
            } catch (error) {
                log(
                    { module: "websocket-rpc", level: "error" },
                    `Failed to remove RPC registrations for socket ${this.socketId}: ${error}`,
                );
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
