import type Database from 'better-sqlite3';

export function applySqlitePragmas(db: Database.Database): void {
    db.pragma('journal_mode = WAL');
    db.pragma('busy_timeout = 5000');
    db.pragma('synchronous = NORMAL');
}
