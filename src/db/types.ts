export type ExecType = 'io' | 'worker';

export interface ClaimedJob {
    id: string;
    name: string;
    type: ExecType;
    payload: string;
    attempts: number;
    maxRetries: number;
}
