import * as zen from 'zendb';
import { Database as SqljsDatabase } from 'sql.js';

export type IDataBase<Schema extends zen.ISchemaAny> = zen.IDatabase<Schema, 'Result'>;

export const Database = (() => {
  return { create };

  function create<Schema extends zen.ISchemaAny>(schema: Schema, db: SqljsDatabase) {
    return zen.Database.create<Schema, 'Result'>(schema, operationResolver);

    function operationResolver(op: zen.IOperation) {
      if (op.kind === 'CreateTable') {
        db.exec(op.sql);
        return;
      }
      if (op.kind === 'Insert') {
        db.prepare(op.sql).run(...op.params);
        return op.parse();
      }
      if (op.kind === 'Delete') {
        const stmt = db.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
        }
        stmt.run();
        stmt.free();
        return op.parse({ deleted: db.getRowsModified() });
      }
      if (op.kind === 'Update') {
        const stmt = db.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
        }
        stmt.run();
        stmt.free();
        return op.parse({ updated: db.getRowsModified() });
      }
      if (op.kind === 'Query') {
        const stmt = db.prepare(op.sql);
        if (op.params) {
          stmt.bind(op.params);
        }
        const results: Array<Record<string, any>> = [];
        while (stmt.step()) {
          results.push(stmt.getAsObject());
        }
        stmt.reset();
        stmt.free();
        return op.parse(results);
      }
      if (op.kind === 'ListTables') {
        const stmt = db.prepare(op.sql);
        const results: Array<Record<string, any>> = [];
        while (stmt.step()) {
          results.push(stmt.getAsObject());
        }
        stmt.reset();
        stmt.free();
        return op.parse(results);
      }
      return expectNever(op);
    }
  }

  function expectNever(val: never): never {
    throw new Error(`Unexpected value: ${val}`);
  }
})();
