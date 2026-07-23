import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

describe("production deployment contract", () => {
    it("execs the application as PID 1 so SIGTERM reaches graceful shutdown", () => {
        const dockerfile = readFileSync(resolve(process.cwd(), "Dockerfile"), "utf8");

        expect(dockerfile).toContain(
            'CMD ["sh", "-c", "npx prisma migrate deploy && exec ./node_modules/.bin/tsx ./sources/main.ts"]',
        );
        expect(dockerfile).not.toMatch(/&&\s+(yarn|npm)\s+(run\s+)?start/);
    });
});
