import fastify from "fastify";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Fastify } from "../types";

const mocks = vi.hoisted(() => ({
    log: vi.fn(),
    verifyToken: vi.fn()
}));

vi.mock("@/utils/log", () => ({ log: mocks.log }));
vi.mock("@/app/auth/auth", () => ({
    auth: { verifyToken: mocks.verifyToken }
}));

import { enableAuthentication } from "./enableAuthentication";
import { enableErrorHandlers } from "./enableErrorHandlers";

describe("HTTP security logging", () => {
    beforeEach(() => {
        mocks.log.mockReset();
        mocks.verifyToken.mockReset();
    });

    it("records authentication metadata without logging the bearer token", async () => {
        const token = "secret-bearer-token-that-must-never-be-logged";
        mocks.verifyToken.mockResolvedValue({ userId: "account-1" });
        const app = fastify();
        const typed = app as unknown as Fastify;
        enableAuthentication(typed);
        app.get("/private", { preHandler: typed.authenticate }, async () => ({ ok: true }));

        try {
            const response = await app.inject({
                method: "GET",
                url: "/private",
                headers: { authorization: `Bearer ${token}` }
            });

            expect(response.statusCode).toBe(200);
            expect(mocks.verifyToken).toHaveBeenCalledWith(token);
            expect(mocks.log).toHaveBeenCalledWith({
                module: "auth-decorator",
                path: "/private",
                hasAuthorization: true,
                bearerScheme: true
            }, "Auth check");
            expect(JSON.stringify(mocks.log.mock.calls)).not.toContain(token);
        } finally {
            await app.close();
        }
    });

    it("records safe 404 metadata without logging authorization headers", async () => {
        const token = "another-secret-bearer-token";
        const app = fastify();
        enableErrorHandlers(app as unknown as Fastify);

        try {
            const response = await app.inject({
                method: "GET",
                url: "/missing?debug=true",
                headers: { authorization: `Bearer ${token}` }
            });

            expect(response.statusCode).toBe(404);
            expect(mocks.log).toHaveBeenCalledWith({
                module: "404-handler",
                method: "GET",
                path: "/missing?debug=true",
                hasAuthorization: true
            }, "Route not found");
            expect(JSON.stringify(mocks.log.mock.calls)).not.toContain(token);
        } finally {
            await app.close();
        }
    });
});
