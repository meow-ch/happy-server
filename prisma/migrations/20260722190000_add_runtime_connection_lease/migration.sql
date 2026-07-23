-- A runtime incarnation identifies the local process; a connection lease
-- identifies one particular socket generation of that process. Both are
-- required to fence a late heartbeat or disconnect from an older connection.
-- Nullable columns preserve rows written before a drained upgrade; rows that
-- have never been lease-managed use a bounded activity fallback. Do not run
-- lease-aware and lease-unaware server writers concurrently.
ALTER TABLE "Session"
    ADD COLUMN "runtimeConnectionLeaseId" TEXT,
    ADD COLUMN "runtimeConnectionLeaseInstanceId" TEXT,
    ADD COLUMN "runtimeConnectionLeaseExpiresAt" TIMESTAMP(3);

-- Each durable prompt is bound to the runtime generation that owned the
-- session when the message committed. A later reconnect repairs through the
-- durable cursor instead of causing the same prompt to execute twice.
ALTER TABLE "SessionMessageNotificationOutbox"
    ADD COLUMN "targetRuntimeConnectionLeaseId" TEXT,
    -- Existing rows came from a lease-unaware writer and need legacy delivery.
    ADD COLUMN "targetLegacyRuntimeConnection" BOOLEAN NOT NULL DEFAULT TRUE;

-- Future omissions must fail closed. The lease-aware writer supplies the
-- intended value explicitly, and mixed-version writers are prohibited.
ALTER TABLE "SessionMessageNotificationOutbox"
    ALTER COLUMN "targetLegacyRuntimeConnection" SET DEFAULT FALSE;

-- Runtime incarnation IDs are single-use. Retaining every displaced or
-- explicitly-ended ID prevents an arbitrarily old reconnect from reviving a
-- terminal session after newer incarnations have come and gone.
CREATE TABLE "SessionRuntimeInstanceRetirement" (
    "sessionId" TEXT NOT NULL,
    "instanceId" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "retiredAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "SessionRuntimeInstanceRetirement_pkey" PRIMARY KEY ("sessionId", "instanceId"),
    CONSTRAINT "SessionRuntimeInstanceRetirement_status_check"
        CHECK ("status" IN ('replaying', 'ended', 'superseded')),
    CONSTRAINT "SessionRuntimeInstanceRetirement_sessionId_fkey"
        FOREIGN KEY ("sessionId") REFERENCES "Session"("id") ON DELETE CASCADE ON UPDATE CASCADE
);
