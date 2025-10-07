import type {
  Driver,
  TCreateTableOperation,
  TDeleteOperation,
  TDropTableOperation,
  TInsertOperation,
  TListTablesOperation,
  TOperation,
  TOperationResult,
  TPragmaOperation,
  TPragmaSetOperation,
  TQueryOperation,
  TUpdateOperation,
} from '@dldc/zendb';
import type { Database, SqlJsStatic } from 'sql.js';

export interface TSqlJsDriver extends Driver.TDriver<Database> {
  sqlJsStatic: null | SqlJsStatic;
  setSqlJsStatic: (sqlJsStatic: SqlJsStatic) => void;
}

export const SqlJsDriver: TSqlJsDriver = {
  sqlJsStatic: null,
  setSqlJsStatic: (sqlJsStatic: SqlJsStatic) => {
    if (SqlJsDriver.sqlJsStatic) {
      console.warn('sql.js is already set. Overriding the existing instance.');
    }
    SqlJsDriver.sqlJsStatic = sqlJsStatic;
  },
  createDatabase: () => {
    if (!SqlJsDriver.sqlJsStatic) {
      throw new Error('sql.js is not set. Please call `setSqlJsStatic` first.');
    }
    return new SqlJsDriver.sqlJsStatic.Database();
  },
  closeDatabase: (db: Database) => {
    db.close();
  },
  exec,
  execMany,
};

function exec<Op extends TOperation>(db: Database, op: Op): TOperationResult<Op> {
  if (op.kind === 'CreateTable') {
    db.exec(op.sql);
    return opResult<TCreateTableOperation>(null);
  }
  if (op.kind === 'DropTable') {
    db.exec(op.sql);
    return opResult<TDropTableOperation>(null);
  }
  if (op.kind === 'Insert') {
    db.exec(op.sql, op.params ? prepareParams(op.params) : null);
    return opResult<TInsertOperation<any>>(op.parse());
  }
  if (op.kind === 'InsertMany') {
    db.exec(op.sql, op.params ? prepareParams(op.params) : null);
    return opResult<TInsertOperation<any>>(op.parse());
  }
  if (op.kind === 'Delete') {
    const stmt = db.prepare(op.sql);
    if (op.params) {
      stmt.bind(prepareParams(op.params));
    }
    stmt.run();
    stmt.free();
    return opResult<TDeleteOperation>(op.parse({ deleted: db.getRowsModified() }));
  }
  if (op.kind === 'Update') {
    const stmt = db.prepare(op.sql);
    if (op.params) {
      stmt.bind(prepareParams(op.params));
    }
    stmt.run();
    stmt.free();
    return opResult<TUpdateOperation>(op.parse({ updated: db.getRowsModified() }));
  }
  if (op.kind === 'Query') {
    const stmt = db.prepare(op.sql);
    if (op.params) {
      stmt.bind(prepareParams(op.params));
    }
    const results: Array<Record<string, any>> = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject());
    }
    stmt.reset();
    stmt.free();
    return opResult<TQueryOperation<any>>(op.parse(results));
  }
  if (op.kind === 'ListTables') {
    const stmt = db.prepare(op.sql);
    const results: Array<Record<string, any>> = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject());
    }
    stmt.reset();
    stmt.free();
    return opResult<TListTablesOperation>(op.parse(results));
  }
  if (op.kind === 'Pragma') {
    const stmt = db.prepare(op.sql);
    stmt.step();
    const result = stmt.getAsObject();
    stmt.reset();
    stmt.free();
    return opResult<TPragmaOperation<any>>(op.parse([result]));
  }
  if (op.kind === 'PragmaSet') {
    db.exec(op.sql);
    return opResult<TPragmaSetOperation>(null);
  }

  return expectNever(op);
}

function execMany<Op extends TOperation>(db: Database, ops: Op[]): TOperationResult<Op>[] {
  return ops.map((op) => exec(db, op));
}

function opResult<Op extends TOperation>(res: TOperationResult<Op>): TOperationResult<TOperation> {
  return res;
}

function expectNever(val: never): never {
  throw new Error(`Unexpected value: ${val as any}`);
}

function mapKeys<T extends Record<string, any>>(obj: T, transformKey: (key: string) => string): T {
  return Object.fromEntries(
    Object.entries(obj).map(([key, val]) => {
      return [transformKey(key), val];
    }),
  ) as any;
}

function prepareParams(params: Record<string, any>): Record<string, any> {
  return mapKeys(params, (key) => `:${key}`);
}
