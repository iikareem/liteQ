export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed';

export interface BackoffConfig {
    type: 'exponential' | 'fixed';
    delay: number;
}

export interface EnqueueOptions {
    delay?: number;
    maxRetries?: number;
    priority?: number;
    backoff?: BackoffConfig;
}

export interface Job<T = unknown> {
    id: string;
    taskType: string;
    data: T;
    attempts: number;
    maxRetries: number;
    status: JobStatus;
    result?: unknown;
    errorLog?: string;
}

export type Enqueuer<T = unknown> = (data: T, options?: EnqueueOptions) => Promise<Job<T>>;

export interface QueueStats {
    pending: number;
    processing: number;
    completed: number;
    failed: number;
    total: number;
}

export interface PurgeOptions {
    olderThan: number;
}

export interface LiteQOptions {
    storagePath: string;
    concurrency?: number;
    pollInterval?: number;
    jobTimeout?: number;
    minWorkers?: number;
    maxWorkers?: number;
}

export type JobHandler<T = unknown> = (job: Job<T>) => Promise<unknown>;

export interface LiteQEvents {
    'job:success': [job: Job];
    'job:failed': [job: Job, errorLog: string];
    'job:retry': [job: Job];
    'queue:start': [];
    'queue:stop': [];
}
