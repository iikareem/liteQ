import { parentPort } from 'node:worker_threads';

parentPort.on('message', async ({ handlerPath, job }) => {
  try {
    const mod = await import(handlerPath);
    const fn = mod.default;
    if (typeof fn !== 'function') {
      throw new Error(`Handler at "${handlerPath}" must export a default function`);
    }
    await fn(job);
    parentPort.postMessage({ status: 'success' });
  } catch (err) {
    parentPort.postMessage({
      status: 'error',
      error: err instanceof Error ? err.message : String(err),
    });
  }
});
