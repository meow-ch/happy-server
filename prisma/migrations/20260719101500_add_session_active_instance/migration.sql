-- Fence durable session-end retries to the runtime incarnation that created
-- them, so an ACK-lost retry cannot terminate a later resumed process.
ALTER TABLE "Session" ADD COLUMN "activeInstanceId" TEXT;
