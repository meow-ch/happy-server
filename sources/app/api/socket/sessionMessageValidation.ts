import { validate as validateUuid } from "uuid";

export type ParsedSessionMessageLocalId =
    | { valid: true; localId: string | null }
    | { valid: false; localId: null };

/** Legacy producers may omit localId; durable producers must use a UUID. */
export function parseSessionMessageLocalId(value: unknown): ParsedSessionMessageLocalId {
    if (value === undefined || value === null) {
        return { valid: true, localId: null };
    }
    if (typeof value !== "string" || value.length !== 36 || !validateUuid(value)) {
        return { valid: false, localId: null };
    }
    return { valid: true, localId: value };
}
