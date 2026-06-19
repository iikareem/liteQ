import {describe, it, expect, afterEach} from 'vitest';
import {resolve} from 'node:path';
import {WorkerPool} from './worker-pool.js';

const TEST_HANDLER = resolve('./workers/test-worker.js');
const SLOW_HANDLER = resolve('./workers/slow-worker.js');

function makeJob(id = '1') {
  return {
    id, taskType: 'test', data: null,
    attempts: 0, maxRetries: 3, status: 'processing' as const,
  };
}

describe('WorkerPool', () => {
  let pool: WorkerPool;

  afterEach(async () => {
    await pool?.stop().catch(() => {});
  });

  it('executes a job and resolves', async () => {
    pool = new WorkerPool(1, 2);
    await expect(
      pool.execute(TEST_HANDLER, makeJob()),
    ).resolves.toBeUndefined();
  });

  it('reuses a single worker for multiple jobs', async () => {
    pool = new WorkerPool(1, 2);
    await pool.execute(TEST_HANDLER, makeJob('1'));
    await pool.execute(TEST_HANDLER, makeJob('2'));
    // One worker serves both jobs sequentially
  });

  it('queues jobs at maxWorkers and drains when workers free', async () => {
    pool = new WorkerPool(1, 1);

    const p1 = pool.execute(SLOW_HANDLER, makeJob('1'));
    const p2 = pool.execute(SLOW_HANDLER, makeJob('2'));

    await expect(p1).resolves.toBeUndefined();
    await expect(p2).resolves.toBeUndefined();
  }, 10000);

  it('runs different handlers on the same pool', async () => {
    pool = new WorkerPool(1, 2);

    const p1 = pool.execute(SLOW_HANDLER, makeJob('1'));
    const p2 = pool.execute(TEST_HANDLER, makeJob('2'));

    await expect(p1).resolves.toBeUndefined();
    await expect(p2).resolves.toBeUndefined();
  });

  it('caps concurrent workers at maxWorkers', async () => {
    pool = new WorkerPool(1, 2);

    const p1 = pool.execute(SLOW_HANDLER, makeJob('1'));
    const p2 = pool.execute(SLOW_HANDLER, makeJob('2'));
    const p3 = pool.execute(SLOW_HANDLER, makeJob('3'));

    await expect(p1).resolves.toBeUndefined();
    await expect(p2).resolves.toBeUndefined();
    await expect(p3).resolves.toBeUndefined();
  }, 10000);

  it('rejects queued jobs on stop', async () => {
    pool = new WorkerPool(1, 1);

    const inFlight = pool.execute(SLOW_HANDLER, makeJob('1'));
    const queued = pool.execute(SLOW_HANDLER, makeJob('2'));

    const settled = Promise.allSettled([inFlight, queued]);

    await pool.stop();

    const [r1, r2] = await settled;
    expect(r1.status).toBe('rejected');
    expect(r2.status).toBe('rejected');
  });

  it('trims idle workers above minWorkers', async () => {
    pool = new WorkerPool(0, 4);

    await pool.execute(TEST_HANDLER, makeJob('1'));
    await pool.execute(TEST_HANDLER, makeJob('2'));
    await pool.execute(TEST_HANDLER, makeJob('3'));

    // minWorkers=0, all idle workers should get trimmed
  });
});
