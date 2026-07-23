import { Redis } from 'ioredis';
import type { RedisOptions } from 'ioredis';

const redisUrl = process.env.REDIS_URL!;

export const redis = new Redis(redisUrl);

// RPC registration is fenced by a PostgreSQL owner transaction. Commands on
// this connection must either complete promptly inside that fence or fail;
// ioredis must never queue/resend them after the transaction releases.
export const RPC_REGISTRATION_REDIS_OPTIONS = {
    lazyConnect: true,
    enableOfflineQueue: false,
    autoResendUnfulfilledCommands: false,
    commandTimeout: 5_000,
    connectTimeout: 5_000,
    maxRetriesPerRequest: 1,
    retryStrategy: (attempt) => Math.min(attempt * 100, 1_000),
} satisfies RedisOptions;

export const rpcRegistrationRedis = new Redis(
    redisUrl,
    RPC_REGISTRATION_REDIS_OPTIONS,
);
