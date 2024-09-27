import type * as zen from '@dldc/zendb';
import type { Database as SqljsDatabase } from 'sql.js';

export interface ISqlJsDatabase {
  exec<Op extends zen.TOperation>(op: Op): zen.TOperationResult<Op>;
  execMany<Op extends zen.TOperation>(ops: Op[]): zen.TOperationResult<Op>[];
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

    function exec<Op extends zen.TOperation>(op: Op): zen.TOperationResult<Op> {
      if (op.kind === 'CreateTable') {
        database.exec(op.sql);
        return opResult<zen.TCreateTableOperation>(null);
      }
      if (op.kind === 'DropTable') {
        database.exec(op.sql);
        return opResult<zen.TDropTableOperation>(null);
      }
      if (op.kind === 'Insert') {
        database.exec(op.sql, op.params ? prepareParams(op.params) : null);
        return opResult<zen.TInsertOperation<any>>(op.parse());
      }
      if (op.kind === 'InsertMany') {
        database.exec(op.sql, op.params ? prepareParams(op.params) : null);
        return opResult<zen.TInsertOperation<any>>(op.parse());
      }
      if (op.kind === 'Delete') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(prepareParams(op.params));
        }
        stmt.run();
        stmt.free();
        return opResult<zen.TDeleteOperation>(op.parse({ deleted: database.getRowsModified() }));
      }
      if (op.kind === 'Update') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(prepareParams(op.params));
        }
        stmt.run();
        stmt.free();
        return opResult<zen.TUpdateOperation>(op.parse({ updated: database.getRowsModified() }));
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
        return opResult<zen.TQueryOperation<any>>(op.parse(results));
      }
      if (op.kind === 'ListTables') {
        const stmt = database.prepare(op.sql);
        const results: Array<Record<string, any>> = [];
        while (stmt.step()) {
          results.push(stmt.getAsObject());
        }
        stmt.reset();
        stmt.free();
        return opResult<zen.TListTablesOperation>(op.parse(results));
      }
      if (op.kind === 'Pragma') {
        const stmt = database.prepare(op.sql);
        stmt.step();
        const result = stmt.getAsObject();
        stmt.reset();
        stmt.free();
        return opResult<zen.TPragmaOperation<any>>(op.parse([result]));
      }
      if (op.kind === 'PragmaSet') {
        database.exec(op.sql);
        return opResult<zen.TPragmaSetOperation>(null);
      }

      return expectNever(op);
    }

    function opResult<Op extends zen.TOperation>(res: zen.TOperationResult<Op>): zen.TOperationResult<zen.TOperation> {
      return res;
    }

    function execMany<Op extends zen.TOperation>(ops: Op[]): zen.TOperationResult<Op>[] {
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
