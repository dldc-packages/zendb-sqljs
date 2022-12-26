import { IDriver, IDriverDatabase, IDriverStatement } from 'zendb';
import { SqlJsStatic, Database, Statement } from 'sql.js';

export type FileStore = { main: Uint8Array | null; migration: Uint8Array | null };

export class Driver implements IDriver<DriverDatabase> {
  public readonly sqlJsStatic: SqlJsStatic;
  public readonly files: FileStore;

  constructor(sqlJsStatic: SqlJsStatic, files: FileStore) {
    this.sqlJsStatic = sqlJsStatic;
    this.files = files;
  }

  openMain(): DriverDatabase {
    return new DriverDatabase(new this.sqlJsStatic.Database(this.files.main));
  }

  openMigration(): DriverDatabase {
    return new DriverDatabase(new this.sqlJsStatic.Database(this.files.migration));
  }

  removeMain(): void {
    this.files.main = null;
  }

  removeMigration(): void {
    this.files.migration = null;
  }

  applyMigration(): void {
    this.files.main = this.files.migration;
    this.files.migration = null;
  }
}

export class DriverDatabase implements IDriverDatabase<DriverStatement> {
  public readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  prepare(source: string): DriverStatement {
    return new DriverStatement(this.db, this.db.prepare(source));
  }

  transaction(fn: () => void): void {
    console.warn('Transactions are not supported by sql.js');
    fn();
  }

  exec(source: string): this {
    this.db.exec(source);
    return this;
  }

  close(): void {
    this.db.close();
  }

  getUserVersion(): number {
    const res = this.db.exec('PRAGMA user_version');
    return res[0].values[0][0] as number;
  }

  setUserVersion(version: number): void {
    this.db.exec(`PRAGMA user_version = ${version}`);
  }
}

export class DriverStatement implements IDriverStatement {
  public readonly statement: Statement;
  public readonly db: Database;

  constructor(db: Database, statement: Statement) {
    this.db = db;
    this.statement = statement;
  }

  run(): { changes: number } {
    this.statement.run();
    const changes = this.db.getRowsModified();
    return { changes: changes };
  }

  all(): any[] {
    const results: Array<Record<string, any>> = [];
    while (this.statement.step()) {
      results.push(this.statement.getAsObject());
    }
    this.statement.reset();
    return results;
  }

  bind(...params: any[]): this {
    this.statement.bind(...params);
    return this;
  }

  free() {
    this.statement.free();
    return;
  }
}
