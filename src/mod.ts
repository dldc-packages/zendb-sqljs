import type * as zen from '@dldc/zendb';
import type { Database as SqljsDatabase } from 'sql.js';

export interface ISqlJsDatabase {
  exec<Op extends zen.IOperation>(op: Op): zen.IOperationResult<Op>;
  execMany<Op extends zen.IOperation>(ops: Op[]): zen.IOperationResult<Op>[];
  readonly database: SqljsDatabase;
}

export const SqlJsDatabase = (() => {
  return { create };

  function create(database: SqljsDatabase) {
    return {
      exec,
      execMany,
      database,
    };

    function exec<Op extends zen.IOperation>(op: Op): zen.IOperationResult<Op> {
      if (op.kind === 'CreateTable') {
        database.exec(op.sql);
        return opResult<zen.ICreateTableOperation>(null);
      }
      if (op.kind === 'Insert') {
        database.exec(op.sql, op.params ? prepareParams(op.params) : null);
        return opResult<zen.IInsertOperation<any>>(op.parse());
      }
      if (op.kind === 'InsertMany') {
        database.exec(op.sql, op.params ? prepareParams(op.params) : null);
        return opResult<zen.IInsertOperation<any>>(op.parse());
      }
      if (op.kind === 'Delete') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(prepareParams(op.params));
        }
        stmt.run();
        stmt.free();
        return opResult<zen.IDeleteOperation>(op.parse({ deleted: database.getRowsModified() }));
      }
      if (op.kind === 'Update') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(prepareParams(op.params));
        }
        stmt.run();
        stmt.free();
        return opResult<zen.IUpdateOperation>(op.parse({ updated: database.getRowsModified() }));
      }
      if (op.kind === 'Query') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(prepareParams(op.params));
        }
        const results: Array<Record<string, any>> = [];
        while (stmt.step()) {
          results.push(stmt.getAsObject());
        }
        stmt.reset();
        stmt.free();
        return opResult<zen.IQueryOperation<any>>(op.parse(results));
      }
      if (op.kind === 'ListTables') {
        const stmt = database.prepare(op.sql);
        const results: Array<Record<string, any>> = [];
        while (stmt.step()) {
          results.push(stmt.getAsObject());
        }
        stmt.reset();
        stmt.free();
        return opResult<zen.IListTablesOperation>(op.parse(results));
      }
      if (op.kind === 'Pragma') {
        const stmt = database.prepare(op.sql);
        stmt.step();
        const result = stmt.getAsObject();
        stmt.reset();
        stmt.free();
        return opResult<zen.IPragmaOperation<any>>(op.parse([result]));
      }
      if (op.kind === 'PragmaSet') {
        database.exec(op.sql);
        return opResult<zen.IPragmaSetOperation>(null);
      }

      return expectNever(op);
    }

    function opResult<Op extends zen.IOperation>(res: zen.IOperationResult<Op>): zen.IOperationResult<zen.IOperation> {
      return res;
    }

    function execMany<Op extends zen.IOperation>(ops: Op[]): zen.IOperationResult<Op>[] {
      return ops.map((op) => exec(op));
    }
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
})();
