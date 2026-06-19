import {Worker} from 'node:worker_threads';
import {fileURLToPath} from 'node:url';
import type {Job} from './types.js';

interface QueuedJob {
    handlerPath: string;
    job: Job;
    resolve: () => void;
    reject: (err: unknown) => void;
}

interface PoolWorker {
    worker: Worker;
    busy: boolean;
    resolve: (() => void) | null;
    reject: ((err: unknown) => void) | null;
}

interface WorkerMessage {
    status: 'success' | 'error';
    error?: string;
}

const GENERIC_WORKER = fileURLToPath(
    new URL('./generic-worker.js', import.meta.url),
);

const POOL_STOPPED = new Error('Worker pool stopped');

export class WorkerPool {
    private workers: PoolWorker[] = [];
    private queue: QueuedJob[] = [];
    private stopping = false;

    constructor(
        private minWorkers: number,
        private maxWorkers: number,
    ) {}

    get canAccept(): boolean {
        return this.findIdle() !== undefined || this.workers.length < this.maxWorkers;
    }

    execute(handlerPath: string, job: Job): Promise<void> {
        const worker = this.acquireWorker();
        if (!worker) {
            return new Promise((resolve, reject) => {
                this.queue.push({handlerPath, job, resolve, reject});
            });
        }
        return this.dispatch(worker, handlerPath, job);
    }

    async stop(): Promise<void> {
        this.stopping = true;

        for (const entry of this.workers) {
            this.rejectPending(entry, POOL_STOPPED);
        }

        for (const queued of this.queue) {
            queued.reject(POOL_STOPPED);
        }
        this.queue = [];

        const workers = this.workers.splice(0);
        await Promise.all(workers.map((entry) => entry.worker.terminate()));
    }

    private findIdle(): PoolWorker | undefined {
        return this.workers.find((worker) => !worker.busy);
    }

    private acquireWorker(): PoolWorker | undefined {
        const idle = this.findIdle();
        if (idle) return idle;
        if (this.workers.length < this.maxWorkers) return this.spawn();
        return undefined;
    }

    private spawn(): PoolWorker {
        const worker = new Worker(GENERIC_WORKER);
        const entry: PoolWorker = {worker, busy: false, resolve: null, reject: null};

        worker.on('message', (msg: WorkerMessage) => {
            if (entry.reject) {
                if (msg.status === 'error') {
                    entry.reject(new Error(msg.error ?? 'Unknown worker error'));
                } else {
                    entry.resolve!();
                }
                this.clearPending(entry);
            }
            entry.busy = false;
            this.processQueue();
        });

        worker.on('error', (err) => {
            this.rejectPending(entry, err);
            entry.busy = false;
            this.remove(entry);
            this.processQueue();
        });

        worker.on('exit', (code) => {
            if (code !== 0) {
                this.rejectPending(entry, new Error(`Worker exited with code ${code}`));
            }
            entry.busy = false;
            this.remove(entry);
            this.processQueue();
        });

        this.workers.push(entry);
        return entry;
    }

    private dispatch(entry: PoolWorker, handlerPath: string, job: Job): Promise<void> {
        entry.busy = true;
        return new Promise((resolve, reject) => {
            entry.resolve = resolve;
            entry.reject = reject;
            entry.worker.postMessage({handlerPath, job});
        });
    }

    private processQueue(): void {
        if (this.stopping) return;

        const remaining: QueuedJob[] = [];

        for (const queued of this.queue) {
            const worker = this.acquireWorker();
            if (worker) {
                this.dispatch(worker, queued.handlerPath, queued.job)
                    .then(queued.resolve)
                    .catch(queued.reject);
            } else {
                remaining.push(queued);
            }
        }

        this.queue = remaining;
        this.trimIdle();
    }

    private trimIdle(): void {
        const idle = this.workers.filter((worker) => !worker.busy);
        const excess = idle.length - this.minWorkers;
        if (excess <= 0) return;

        for (const entry of idle.slice(0, excess)) {
            this.remove(entry);
            entry.worker.terminate();
        }
    }

    private remove(entry: PoolWorker): void {
        const index = this.workers.indexOf(entry);
        if (index >= 0) this.workers.splice(index, 1);
    }

    private clearPending(entry: PoolWorker): void {
        entry.resolve = null;
        entry.reject = null;
    }

    private rejectPending(entry: PoolWorker, err: unknown): void {
        if (!entry.reject) return;
        entry.reject(err);
        this.clearPending(entry);
    }
}
