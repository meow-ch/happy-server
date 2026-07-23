export async function delay(ms: number, signal?: AbortSignal): Promise<void> {
    if (!signal) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    if (signal.aborted) {
        return;
    }
    
    await new Promise<void>((resolve) => {
        let settled = false;
        let timeout: ReturnType<typeof setTimeout>;

        const finish = () => {
            if (settled) {
                return;
            }
            settled = true;
            clearTimeout(timeout);
            signal.removeEventListener('abort', abortHandler);
            resolve();
        };

        const abortHandler = () => finish();
        timeout = setTimeout(finish, ms);
        signal.addEventListener('abort', abortHandler, { once: true });

        // Close the race between the initial aborted check and listener setup.
        if (signal.aborted) {
            finish();
        }
    });
}
