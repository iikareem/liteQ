import {randomUUID} from 'node:crypto';
import {resolve} from 'node:path';
import type {ClaimedJob, ExecType} from '../db/index.js';
import type {EnqueueOptions, Enqueuer, Job, JobHandler, PurgeOptions, QueueStats} from '../types.js';
import {DEFAULT_MAX_RETRIES, DEFAULT_PRIORITY} from './constants.js';
import type {QueueContext} from './context.js';
import {executeHandler, withTimeout} from './executor.js';
import {toJob} from './mappers.js';

export class JobRunner {
    constructor(private readonly ctx: QueueContext) {}

    register<T>(type: string, handler: JobHandler<T>): Enqueuer<T>;
    register(type: string, workerPath: string): Enqueuer<unknown>;
    register<T>(type: string, handlerOrPath: JobHandler<T> | string): Enqueuer<T> {
        if (typeof handlerOrPath === 'string') {
            this.ctx.handlers.set(type, resolve(handlerOrPath));
            return (data: T, options?: EnqueueOptions) =>
                this.enqueue(type, data, options, 'worker');
        }

        this.ctx.handlers.set(type, handlerOrPath as JobHandler);
        return (data: T, options?: EnqueueOptions) =>
            this.enqueue(type, data, options, 'io');
    }

    async stats(): Promise<QueueStats> {
        const rows = this.ctx.db.stats();
        let total = 0;
        const stats: QueueStats = {pending: 0, processing: 0, completed: 0, failed: 0, total: 0};

        for (const row of rows) {
            const count = Number(row.count);
            if (row.status === 'pending') stats.pending = count;
            else if (row.status === 'processing') stats.processing = count;
            else if (row.status === 'completed') stats.completed = count;
            else if (row.status === 'failed') stats.failed = count;
            total += count;
        }

        stats.total = total;
        return stats;
    }

    async purge(options: PurgeOptions): Promise<void> {
        this.ctx.db.purge(options.olderThan);
    }

    tickIo(): void {
        if (this.ctx.activeIo.count >= this.ctx.concurrency) return;

        const claimed = this.ctx.db.claimNext(Date.now(), this.ctx.jobTimeout, 'io');
        if (!claimed) return;

        this.ctx.activeIo.count++;
        this.run(claimed).finally(() => this.ctx.activeIo.count--);
    }

    tickCpu(): void {
        if (!this.ctx.pool.canAccept) return;

        const claimed = this.ctx.db.claimNext(Date.now(), this.ctx.jobTimeout, 'worker');
        if (!claimed) return;

        this.run(claimed);
    }

    private async enqueue<T>(
        name: string,
        data: T,
        options: EnqueueOptions | undefined,
        execType: ExecType,
    ): Promise<Job<T>> {
        const id = randomUUID();
        const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
        const runAt = Date.now() + (options?.delay ?? 0);

        this.ctx.db.enqueue({
            id,
            name,
            type: execType,
            payload: JSON.stringify(data),
            runAt,
            maxRetries,
            priority: options?.priority ?? DEFAULT_PRIORITY,
        });

        return {
            id,
            taskType: name,
            data,
            attempts: 0,
            maxRetries,
            status: 'pending',
        };
    }

    private async run(claimed: ClaimedJob): Promise<void> {
        const handler = this.ctx.handlers.get(claimed.name);
        if (!handler) {
            this.ctx.db.fail(
                claimed.id,
                `No handler registered for job type: ${claimed.name}`,
                Date.now(),
            );
            return;
        }

        const job = toJob(claimed, JSON.parse(claimed.payload));

        try {
            await withTimeout(
                executeHandler(this.ctx.pool, handler, job),
                this.ctx.jobTimeout,
            );
            this.ctx.db.complete(claimed.id, Date.now());
        } catch (err) {
            this.handleFailure(claimed, err);
        }
    }

    private handleFailure(claimed: ClaimedJob, err: unknown): void {
        const errorMsg = err instanceof Error ? err.message : String(err);

        if (claimed.attempts < claimed.maxRetries) {
            const backoffMs = Math.pow(2, claimed.attempts) * 1000;
            this.ctx.db.retry(claimed.id, Date.now() + backoffMs);
            return;
        }

        this.ctx.db.fail(claimed.id, errorMsg, Date.now());
    }
}
