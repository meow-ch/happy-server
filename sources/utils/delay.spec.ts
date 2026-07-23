import { getEventListeners } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { delay } from './delay';

describe('delay', () => {
    afterEach(() => {
        vi.useRealTimers();
    });

    it('removes the abort listener after the timer completes', async () => {
        vi.useFakeTimers();
        const controller = new AbortController();

        const pending = delay(1_000, controller.signal);
        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(1);

        await vi.advanceTimersByTimeAsync(1_000);
        await pending;

        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
        expect(vi.getTimerCount()).toBe(0);
    });

    it('removes the abort listener and timer when aborted', async () => {
        vi.useFakeTimers();
        const controller = new AbortController();

        const pending = delay(1_000, controller.signal);
        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(1);

        controller.abort();
        await pending;

        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
        expect(vi.getTimerCount()).toBe(0);
    });

    it('returns immediately without registering when already aborted', async () => {
        const controller = new AbortController();
        controller.abort();

        await delay(1_000, controller.signal);

        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
    });
});
