import { onShutdown } from "@/utils/shutdown";
import { Fastify } from "./types";
import { buildMachineActivityEphemeral, ClientConnection, eventRouter, getConnectionRooms } from "@/app/events/eventRouter";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { redis } from "@/storage/redis";
import { log } from "@/utils/log";
import { auth } from "@/app/auth/auth";
import { decrementWebSocketConnection, incrementWebSocketConnection, websocketEventsCounter } from "../monitoring/metrics2";
import { usageHandler } from "./socket/usageHandler";
import { rpcHandler } from "./socket/rpcHandler";
import { pingHandler } from "./socket/pingHandler";
import { sessionUpdateHandler } from "./socket/sessionUpdateHandler";
import { machineUpdateHandler } from "./socket/machineUpdateHandler";
import { artifactUpdateHandler } from "./socket/artifactUpdateHandler";
import { accessKeyHandler } from "./socket/accessKeyHandler";
import { SessionMessageNotificationBus } from "@/app/session/sessionMessageNotificationBus";
import {
    SessionMessageNotificationDispatcher,
    sessionMessageNotificationRepository
} from "@/app/session/sessionMessageNotificationOutbox";
import { parseSessionMessageLocalId } from "@/app/api/socket/sessionMessageValidation";
import { db } from "@/storage/db";
import type { RpcHandlerLifecycle } from "@/app/api/socket/rpcHandler";
import { shutdownSocketRuntime } from "@/app/api/socket/shutdownSocketRuntime";

export async function startSocket(app: Fastify) {
    const socketPublisherRedis = redis.duplicate({ lazyConnect: true });
    const socketSubscriberRedis = redis.duplicate({ lazyConnect: true });
    const notificationPublisherRedis = redis.duplicate({ lazyConnect: true });
    const notificationSubscriberRedis = redis.duplicate({ lazyConnect: true });
    const realtimeRedisClients = [
        socketPublisherRedis,
        socketSubscriberRedis,
        notificationPublisherRedis,
        notificationSubscriberRedis,
    ];
    // Connect each Redis transport before constructing the adapters.
    await Promise.all(realtimeRedisClients.map((client) => client.connect()));
    const rpcLifecycles = new Set<RpcHandlerLifecycle>();
    const io = new Server(app.server, {
        cors: {
            origin: "*",
            methods: ["GET", "POST", "OPTIONS"],
            credentials: true,
            allowedHeaders: ["*"]
        },
        transports: ['websocket', 'polling'],
        pingTimeout: 45000,
        pingInterval: 15000,
        path: '/v1/updates',
        allowUpgrades: true,
        upgradeTimeout: 10000,
        connectTimeout: 20000,
        maxHttpBufferSize: 8 * 1024 * 1024,
        serveClient: false, // Don't serve the client files
        adapter: createAdapter(socketPublisherRedis, socketSubscriberRedis, {
            key: process.env.SOCKET_IO_REDIS_CHANNEL_PREFIX || "happy:socket.io",
            publishOnSpecificResponseChannel: true,
        })
    });
    // The adapter constructor queues its PSUBSCRIBE/SUBSCRIBE calls without
    // returning their promises. A PING on the same subscriber connection is an
    // ordering barrier: when it resolves, every earlier subscription is live.
    await socketSubscriberRedis.ping();
    eventRouter.setServer(io);
    const notificationBus = new SessionMessageNotificationBus(
        notificationPublisherRedis,
        notificationSubscriberRedis,
        process.env.SESSION_MESSAGE_NOTIFICATION_REDIS_CHANNEL || "happy:session-message-notifications",
        async (notification) => eventRouter.emitDurableSessionMessageLocal(notification)
    );
    await notificationBus.start();
    const notificationDispatcher = new SessionMessageNotificationDispatcher(
        sessionMessageNotificationRepository,
        (notification) => notificationBus.publish(notification)
    );
    notificationDispatcher.start();

    // Authenticate before CONNECT and before installing event handlers. Socket.IO
    // connection-state recovery stays disabled because it restores rooms and
    // emits missed packets before namespace middleware receives handshake auth.
    io.use(async (socket, next) => {
        try {
            const token = socket.handshake.auth.token;
            const clientType = socket.handshake.auth.clientType as 'session-scoped' | 'user-scoped' | 'machine-scoped' | undefined;
            const sessionId = socket.handshake.auth.sessionId;
            const machineId = socket.handshake.auth.machineId;
            const parsedSessionInstanceId = parseSessionMessageLocalId(socket.handshake.auth.sessionInstanceId);
            const replayOnly = socket.handshake.auth.replayOnly === true;
            if (typeof token !== 'string' || token.length === 0) {
                next(new Error('Missing authentication token'));
                return;
            }
            if (clientType === 'session-scoped' && (typeof sessionId !== 'string' || sessionId.length === 0)) {
                next(new Error('Session ID required for session-scoped clients'));
                return;
            }
            if (clientType === 'machine-scoped' && (typeof machineId !== 'string' || machineId.length === 0)) {
                next(new Error('Machine ID required for machine-scoped clients'));
                return;
            }
            if (!parsedSessionInstanceId.valid) {
                next(new Error('Invalid session instance ID'));
                return;
            }

            const verified = await auth.verifyToken(token);
            if (!verified) {
                next(new Error('Invalid authentication token'));
                return;
            }
            if (socket.recovered && socket.data.userId && socket.data.userId !== verified.userId) {
                next(new Error('Recovered session account mismatch'));
                return;
            }
            if (clientType === 'session-scoped'
                && typeof sessionId === 'string'
                && parsedSessionInstanceId.localId) {
                // Claim the runtime incarnation before Socket.IO emits CONNECT.
                // Awaiting this inside the connection callback would leave a
                // window where client packets arrive before handlers exist.
                const claimed = replayOnly
                    ? await db.session.updateMany({
                        where: {
                            id: sessionId,
                            accountId: verified.userId,
                            activeInstanceId: null,
                        },
                        data: { activeInstanceId: parsedSessionInstanceId.localId },
                    })
                    : await db.session.updateMany({
                        where: { id: sessionId, accountId: verified.userId },
                        data: {
                            activeInstanceId: parsedSessionInstanceId.localId,
                            active: true,
                            lastActiveAt: new Date(),
                        },
                    });
                if (!replayOnly && claimed.count === 0) {
                    next(new Error('Session not found'));
                    return;
                }
            }
            socket.data.userId = verified.userId;
            socket.data.sessionInstanceId = parsedSessionInstanceId.localId;
            socket.data.replayOnly = replayOnly;
            next();
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Socket authentication failed: ${error}`);
            next(new Error('Authentication failed'));
        }
    });

    io.engine.on("connection_error", (err) => {
        log(
            { module: 'websocket', level: 'error' },
            `Socket connection error code=${err.code} message=${err.message} context=${JSON.stringify(err.context ?? {})}`
        );
    });

    io.on("connection", async (socket) => {
        log({ module: 'websocket' }, `New connection attempt from socket: ${socket.id}`);
        const clientType = socket.handshake.auth.clientType as 'session-scoped' | 'user-scoped' | 'machine-scoped' | undefined;
        const sessionId = socket.handshake.auth.sessionId as string | undefined;
        const machineId = socket.handshake.auth.machineId as string | undefined;
        const userId = socket.data.userId as string;
        const sessionInstanceId = socket.data.sessionInstanceId as string | null | undefined;
        const replayOnly = socket.data.replayOnly === true;
        log({ module: 'websocket' }, `Token verified: ${userId}, clientType: ${clientType || 'user-scoped'}, sessionId: ${sessionId || 'none'}, machineId: ${machineId || 'none'}, socketId: ${socket.id}`);

        // Store connection based on type
        const metadata = { clientType: clientType || 'user-scoped', sessionId, machineId };
        let connection: ClientConnection;
        if (metadata.clientType === 'session-scoped' && sessionId) {
            connection = {
                connectionType: 'session-scoped',
                socket,
                userId,
                sessionId,
                sessionInstanceId: sessionInstanceId ?? undefined,
                replayOnly,
            };
        } else if (metadata.clientType === 'machine-scoped' && machineId) {
            connection = {
                connectionType: 'machine-scoped',
                socket,
                userId,
                machineId
            };
        } else {
            connection = {
                connectionType: 'user-scoped',
                socket,
                userId
            };
        }
        await socket.join(getConnectionRooms(connection));
        eventRouter.addConnection(userId, connection);
        incrementWebSocketConnection(connection.connectionType);

        // Broadcast daemon online status
        if (connection.connectionType === 'machine-scoped') {
            // Broadcast daemon online
            const machineActivity = buildMachineActivityEphemeral(machineId!, true, Date.now());
            eventRouter.emitEphemeral({
                userId,
                payload: machineActivity,
                recipientFilter: { type: 'user-scoped-only' }
            });
        }

        socket.on('disconnect', () => {
            websocketEventsCounter.inc({ event_type: 'disconnect' });

            // Cleanup connections
            eventRouter.removeConnection(userId, connection);
            decrementWebSocketConnection(connection.connectionType);

            log({ module: 'websocket' }, `User disconnected: ${userId}`);

            // Broadcast daemon offline status
            if (connection.connectionType === 'machine-scoped') {
                const machineActivity = buildMachineActivityEphemeral(connection.machineId, false, Date.now());
                eventRouter.emitEphemeral({
                    userId,
                    payload: machineActivity,
                    recipientFilter: { type: 'user-scoped-only' }
                });
            }
        });

        // Handlers
        const rpcLifecycle = rpcHandler(userId, socket);
        rpcLifecycles.add(rpcLifecycle);
        socket.on("disconnect", () => {
            void rpcLifecycle.close().finally(() => rpcLifecycles.delete(rpcLifecycle));
        });
        usageHandler(userId, socket);
        sessionUpdateHandler(userId, socket, connection, notificationDispatcher);
        pingHandler(socket);
        machineUpdateHandler(userId, socket);
        artifactUpdateHandler(userId, socket);
        accessKeyHandler(userId, socket);

        // Ready
        log({ module: 'websocket' }, `User connected: ${userId}`);
    });

    onShutdown('api', async () => {
        await shutdownSocketRuntime({
            io,
            rpcLifecycles,
            notificationDispatcher,
            notificationBus,
            redisClients: [...realtimeRedisClients, redis],
        });
    });
}
