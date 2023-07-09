import { Database } from '@dldc/zendb';
import initSqlJs, { SqlJsStatic } from 'sql.js';
import { beforeAll, expect, test } from 'vitest';
import { DqlJsDatabase } from '../src/mod';

let sqlJs: SqlJsStatic;

beforeAll(async () => {
  sqlJs = await initSqlJs();
});

test('read pragma', () => {
  const sqldb = new sqlJs.Database();
  const db = DqlJsDatabase.create(sqldb);

  const res = db.exec(Database.userVersion());
  expect(res).toEqual(0);
});

test('write pragma', () => {
  const sqldb = new sqlJs.Database();
  const db = DqlJsDatabase.create(sqldb);

  const res = db.exec(Database.setUserVersion(42));
  expect(res).toEqual(null);
  const version = db.exec(Database.userVersion());
  expect(version).toEqual(42);
});
