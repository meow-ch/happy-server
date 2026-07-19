import { describe, expect, it } from "vitest";
import { parseSessionMessageLocalId } from "@/app/api/socket/sessionMessageValidation";

describe("parseSessionMessageLocalId", () => {
    it("preserves omitted local IDs for legacy producers", () => {
        expect(parseSessionMessageLocalId(undefined)).toEqual({ valid: true, localId: null });
        expect(parseSessionMessageLocalId(null)).toEqual({ valid: true, localId: null });
    });

    it("accepts a canonical UUID", () => {
        const localId = "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3";
        expect(parseSessionMessageLocalId(localId)).toEqual({ valid: true, localId });
    });

    it.each(["", "local-1", "4de09f61-dc78-4d4f-8a20-6a72c44cb3e3-extra", 42])(
        "rejects unbounded or non-UUID local ID %s",
        (localId) => {
            expect(parseSessionMessageLocalId(localId)).toEqual({ valid: false, localId: null });
        }
    );
});
