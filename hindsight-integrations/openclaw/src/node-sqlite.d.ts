// Minimal ambient declaration for Node's built-in SQLite (`node:sqlite`). The plugin's
// pinned @types/node (20.x) predates the module; the gateway runs Node 24.20, where it is
// available. Only the surface the session ledger uses is declared — plan v3 §6 pins this
// driver precisely so the plugin gains no native dependency (R2).
declare module "node:sqlite" {
  export interface StatementSync {
    run(...params: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
    get(...params: unknown[]): unknown;
    all(...params: unknown[]): unknown[];
  }

  export interface DatabaseSyncOptions {
    open?: boolean;
    readOnly?: boolean;
    enableForeignKeyConstraints?: boolean;
    enableDoubleQuotedStringLiterals?: boolean;
    allowExtension?: boolean;
  }

  export class DatabaseSync {
    constructor(path: string, options?: DatabaseSyncOptions);
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}
