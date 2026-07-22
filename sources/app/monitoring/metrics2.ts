import { register, Counter, Gauge, Histogram } from 'prom-client';
import { db } from '@/storage/db';
import { forever } from '@/utils/forever';
import { delay } from '@/utils/delay';
import { shutdownSignal } from '@/utils/shutdown';

// Application metrics
export const websocketConnectionsGauge = new Gauge({
    name: 'websocket_connections_total',
    help: 'Number of active WebSocket connections',
    labelNames: ['type'] as const,
    registers: [register]
});

export const sessionAliveEventsCounter = new Counter({
    name: 'session_alive_events_total',
    help: 'Total number of session-alive events',
    registers: [register]
});

export const sessionActivityPublicationsCounter = new Counter({
    name: 'session_activity_publications_total',
    help: 'Session activity publications and heartbeats coalesced before Redis fanout',
    labelNames: ['result', 'reason'] as const,
    registers: [register]
});

export const machineAliveEventsCounter = new Counter({
    name: 'machine_alive_events_total',
    help: 'Total number of machine-alive events',
    registers: [register]
});

export const sessionCacheCounter = new Counter({
    name: 'session_cache_operations_total',
    help: 'Total session cache operations',
    labelNames: ['operation', 'result'] as const,
    registers: [register]
});

export const databaseUpdatesSkippedCounter = new Counter({
    name: 'database_updates_skipped_total',
    help: 'Number of database updates skipped due to debouncing',
    labelNames: ['type'] as const,
    registers: [register]
});

export const activityCacheFlushCounter = new Counter({
    name: 'activity_cache_flushes_total',
    help: 'Activity cache flush lifecycle outcomes',
    labelNames: ['result'] as const,
    registers: [register]
});

export const activityCacheDatabaseUpdatesCounter = new Counter({
    name: 'activity_cache_database_updates_total',
    help: 'Activity cache database update outcomes',
    labelNames: ['type', 'result'] as const,
    registers: [register]
});

export const activityCachePendingUpdatesGauge = new Gauge({
    name: 'activity_cache_pending_updates',
    help: 'Activity updates currently pending in memory',
    labelNames: ['type'] as const,
    registers: [register]
});

export const activityCacheFlushInProgressGauge = new Gauge({
    name: 'activity_cache_flush_in_progress',
    help: 'Whether an activity cache flush is currently running',
    registers: [register]
});

export const activityCacheFlushDurationHistogram = new Histogram({
    name: 'activity_cache_flush_duration_seconds',
    help: 'Duration of single-flight activity cache flushes',
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10],
    registers: [register]
});

export const rpcRegistrationRefreshCounter = new Counter({
    name: 'rpc_registration_refreshes_total',
    help: 'RPC registration lease refresh outcomes',
    labelNames: ['result'] as const,
    registers: [register]
});

export const rpcRegistrationRefreshBatchSizeHistogram = new Histogram({
    name: 'rpc_registration_refresh_batch_size',
    help: 'Number of RPC methods refreshed in one Redis command',
    buckets: [1, 2, 4, 8, 16, 32, 64, 128, 256],
    registers: [register]
});

export const websocketEventsCounter = new Counter({
    name: 'websocket_events_total',
    help: 'Total WebSocket events received by type',
    labelNames: ['event_type'] as const,
    registers: [register]
});

export const httpRequestsCounter = new Counter({
    name: 'http_requests_total',
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'route', 'status'] as const,
    registers: [register]
});

export const httpRequestDurationHistogram = new Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request duration in seconds',
    labelNames: ['method', 'route', 'status'] as const,
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10],
    registers: [register]
});

// Database count metrics
export const databaseRecordCountGauge = new Gauge({
    name: 'database_records_total',
    help: 'Total number of records in database tables',
    labelNames: ['table'] as const,
    registers: [register]
});

// WebSocket connection tracking
const connectionCounts = {
    'user-scoped': 0,
    'session-scoped': 0,
    'machine-scoped': 0
};

export function incrementWebSocketConnection(type: 'user-scoped' | 'session-scoped' | 'machine-scoped'): void {
    connectionCounts[type]++;
    websocketConnectionsGauge.set({ type }, connectionCounts[type]);
}

export function decrementWebSocketConnection(type: 'user-scoped' | 'session-scoped' | 'machine-scoped'): void {
    connectionCounts[type] = Math.max(0, connectionCounts[type] - 1);
    websocketConnectionsGauge.set({ type }, connectionCounts[type]);
}

// Database metrics updater
export async function updateDatabaseMetrics(): Promise<void> {
    // Query counts for each table
    const [accountCount, sessionCount, messageCount, machineCount] = await Promise.all([
        db.account.count(),
        db.session.count(),
        db.sessionMessage.count(),
        db.machine.count()
    ]);

    // Update metrics
    databaseRecordCountGauge.set({ table: 'accounts' }, accountCount);
    databaseRecordCountGauge.set({ table: 'sessions' }, sessionCount);
    databaseRecordCountGauge.set({ table: 'messages' }, messageCount);
    databaseRecordCountGauge.set({ table: 'machines' }, machineCount);
}

export function startDatabaseMetricsUpdater(): void {
    forever('database-metrics-updater', async () => {
        await updateDatabaseMetrics();
        
        // Wait 60 seconds before next update
        await delay(60 * 1000, shutdownSignal);
    });
}

// Export the register for combining metrics
export { register };
