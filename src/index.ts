import * as zen from 'zendb';
import { Database as SqljsDatabase } from 'sql.js';

export interface IDataBase {
  exec<Op extends zen.IOperation>(op: Op): zen.IOperationResult<Op>;
  execMany<Op extends zen.IOperation>(ops: Op[]): zen.IOperationResult<Op>[];
  readonly database: SqljsDatabase;
}

export const Database = (() => {
  return { create, listTables: zen.Database.listTables, createTables: zen.Database.createTables };

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
        database.exec(op.sql, op.params);
        return opResult<zen.IInsertOperation<any>>(op.parse());
      }
      if (op.kind === 'Delete') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
        }
        stmt.run();
        stmt.free();
        return opResult<zen.IDeleteOperation>(op.parse({ deleted: database.getRowsModified() }));
      }
      if (op.kind === 'Update') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
        }
        stmt.run();
        stmt.free();
        return opResult<zen.IUpdateOperation>(op.parse({ updated: database.getRowsModified() }));
      }
      if (op.kind === 'Query') {
        const stmt = database.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
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
      return expectNever(op);
    }

    function opResult<Op extends zen.IOperation>(
      res: zen.IOperationResult<Op>
    ): zen.IOperationResult<zen.IOperation> {
      return res;
    }

    function execMany<Op extends zen.IOperation>(ops: Op[]): zen.IOperationResult<Op>[] {
      return ops.map((op) => exec(op));
    }
  }

  function expectNever(val: never): never {
    throw new Error(`Unexpected value: ${val}`);
  }
})();
