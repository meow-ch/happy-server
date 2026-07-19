-- A committed session message and its notification are created in one
-- transaction. Delivery may happen more than once, so the outbox id is also
-- the stable update id consumed by clients.
CREATE TABLE "SessionMessageNotificationOutbox" (
    "id" TEXT NOT NULL,
    "accountId" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "messageId" TEXT NOT NULL,
    "updateSeq" INTEGER NOT NULL,
    "originSocketId" TEXT,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "nextAttemptAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "claimedAt" TIMESTAMP(3),
    "claimToken" TEXT,
    "deliveredAt" TIMESTAMP(3),
    "lastError" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SessionMessageNotificationOutbox_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "SessionMessageNotificationOutbox_messageId_key"
    ON "SessionMessageNotificationOutbox"("messageId");
CREATE INDEX "SessionMessageNotificationOutbox_deliveredAt_nextAttemptAt_idx"
    ON "SessionMessageNotificationOutbox"("deliveredAt", "nextAttemptAt");
CREATE INDEX "SessionMessageNotificationOutbox_claimedAt_idx"
    ON "SessionMessageNotificationOutbox"("claimedAt");
CREATE INDEX "SessionMessageNotificationOutbox_sessionId_deliveredAt_idx"
    ON "SessionMessageNotificationOutbox"("sessionId", "deliveredAt");

ALTER TABLE "SessionMessageNotificationOutbox"
    ADD CONSTRAINT "SessionMessageNotificationOutbox_messageId_fkey"
    FOREIGN KEY ("messageId") REFERENCES "SessionMessage"("id")
    ON DELETE CASCADE ON UPDATE CASCADE;
