#include <stdio.h>
#include "extralite.h"

VALUE cError;
VALUE cSQLError;
VALUE cBusyError;

ID ID_KEYS;
ID ID_STRIP;
ID ID_TO_S;

static size_t Database_size(const void *ptr) {
  return sizeof(Database_t);
}

static void Database_free(void *ptr) {
  Database_t *db = ptr;
  if (db->sqlite3_db) {
    sqlite3_close(db->sqlite3_db);
  }
  // close db
  free(ptr);
}

static const rb_data_type_t Database_type = {
    "Database",
    {0, Database_free, Database_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Database_allocate(VALUE klass) {
  Database_t *db = ALLOC(Database_t);
  db->sqlite3_db = 0;
  return TypedData_Wrap_Struct(klass, &Database_type, db);
}

#define GetDatabase(obj, database) \
  TypedData_Get_Struct((obj), Database_t, &Database_type, (database))

// make sure the database is open
#define GetOpenDatabase(obj, database) { \
  TypedData_Get_Struct((obj), Database_t, &Database_type, (database)); \
  if (!(database)->sqlite3_db) { \
    rb_raise(cError, "Database is closed"); \
  } \
}

/* call-seq: sqlite3_version
 *
 * Returns the sqlite3 version used by Extralite.
 */

VALUE Extralite_sqlite3_version(VALUE self) {
  return rb_str_new_cstr(sqlite3_version);
}

/* call-seq: initialize(path)
 *
 * Initializes a new SQLite database with the given path.
 */

VALUE Database_initialize(VALUE self, VALUE path) {
  int rc;
  Database_t *db;
  GetDatabase(self, db);

  rc = sqlite3_open(StringValueCStr(path), &db->sqlite3_db);
  if (rc) {
    sqlite3_close(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  rc = sqlite3_enable_load_extension(db->sqlite3_db, 1);
  if (rc) {
    sqlite3_close(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  return Qnil;
}

/* call-seq: close
 *
 * Closes the database.
 */
VALUE Database_close(VALUE self) {
  int rc;
  Database_t *db;
  GetDatabase(self, db);

  rc = sqlite3_close(db->sqlite3_db);
  if (rc) {
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  db->sqlite3_db = 0;
  return self;
}

/* call-seq: closed?
 *
 * Returns true if the database is closed.
 */
VALUE Database_closed_p(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);

  return db->sqlite3_db ? Qfalse : Qtrue;
}

/* call-seq:
 *    query(sql, *parameters, &block)
 *    query_hash(sql, *parameters, &block)
 *
 * Runs a query returning rows as hashes (with symbol keys). If a block is
 * given, it will be called for each row. Otherwise, an array containing all
 * rows is returned.
 * 
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as a least, the query should specify parameters using
 * `?`:
 * 
 *     db.query('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 * 
 *     db.query('select * from foo where x = :bar', bar: 42)
 *     db.query('select * from foo where x = :bar', 'bar' => 42)
 *     db.query('select * from foo where x = :bar', ':bar' => 42)
 */
VALUE Database_query_hash(int argc, VALUE *argv, VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);
  query_ctx ctx = { self, db->sqlite3_db, argc, argv, 0 };
  return rb_ensure(SAFE(safe_query_hash), (VALUE)&ctx, cleanup_stmt, (VALUE)&ctx);
}

/* call-seq: query_ary(sql, *parameters, &block)
 *
 * Runs a query returning rows as arrays. If a block is given, it will be called
 * for each row. Otherwise, an array containing all rows is returned.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as a least, the query should specify parameters using
 * `?`:
 *
 *     db.query_ary('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     db.query_ary('select * from foo where x = :bar', bar: 42)
 *     db.query_ary('select * from foo where x = :bar', 'bar' => 42)
 *     db.query_ary('select * from foo where x = :bar', ':bar' => 42)
 */
VALUE Database_query_ary(int argc, VALUE *argv, VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);
  query_ctx ctx = { self, db->sqlite3_db, argc, argv, 0 };
  return rb_ensure(SAFE(safe_query_ary), (VALUE)&ctx, cleanup_stmt, (VALUE)&ctx);
}

/* call-seq: query_single_row(sql, *parameters)
 *
 * Runs a query returning a single row as a hash.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as a least, the query should specify parameters using
 * `?`:
 *
 *     db.query_single_row('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     db.query_single_row('select * from foo where x = :bar', bar: 42)
 *     db.query_single_row('select * from foo where x = :bar', 'bar' => 42)
 *     db.query_single_row('select * from foo where x = :bar', ':bar' => 42)
 */
VALUE Database_query_single_row(int argc, VALUE *argv, VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);
  query_ctx ctx = { self, db->sqlite3_db, argc, argv, 0 };
  return rb_ensure(SAFE(safe_query_single_row), (VALUE)&ctx, cleanup_stmt, (VALUE)&ctx);
}

/* call-seq: query_single_column(sql, *parameters, &block)
 *
 * Runs a query returning single column values. If a block is given, it will be called
 * for each value. Otherwise, an array containing all values is returned.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as a least, the query should specify parameters using
 * `?`:
 *
 *     db.query_single_column('select x from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     db.query_single_column('select x from foo where x = :bar', bar: 42)
 *     db.query_single_column('select x from foo where x = :bar', 'bar' => 42)
 *     db.query_single_column('select x from foo where x = :bar', ':bar' => 42)
 */
VALUE Database_query_single_column(int argc, VALUE *argv, VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);
  query_ctx ctx = { self, db->sqlite3_db, argc, argv, 0 };
  return rb_ensure(SAFE(safe_query_single_column), (VALUE)&ctx, cleanup_stmt, (VALUE)&ctx);
}

/* call-seq: query_single_value(sql, *parameters)
 *
 * Runs a query returning a single value from the first row.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as a least, the query should specify parameters using
 * `?`:
 *
 *     db.query_single_value('select x from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     db.query_single_value('select x from foo where x = :bar', bar: 42)
 *     db.query_single_value('select x from foo where x = :bar', 'bar' => 42)
 *     db.query_single_value('select x from foo where x = :bar', ':bar' => 42)
 */
VALUE Database_query_single_value(int argc, VALUE *argv, VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);
  query_ctx ctx = { self, db->sqlite3_db, argc, argv, 0 };
  return rb_ensure(SAFE(safe_query_single_value), (VALUE)&ctx, cleanup_stmt, (VALUE)&ctx);
}

/* call-seq: last_insert_rowid
 *
 * Returns the rowid of the last inserted row.
 */
VALUE Database_last_insert_rowid(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return INT2NUM(sqlite3_last_insert_rowid(db->sqlite3_db));
}

/* call-seq: changes
 *
 * Returns the number of changes made to the database by the last operation.
 */
VALUE Database_changes(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return INT2NUM(sqlite3_changes(db->sqlite3_db));
}

/* call-seq: filename
 *
 * Returns the database filename.
 */
VALUE Database_filename(int argc, VALUE *argv, VALUE self) {
  const char *db_name;
  const char *filename;
  Database_t *db;
  GetOpenDatabase(self, db);

  rb_check_arity(argc, 0, 1);
  db_name = (argc == 1) ? StringValueCStr(argv[0]) : "main";
  filename = sqlite3_db_filename(db->sqlite3_db, db_name);
  return filename ? rb_str_new_cstr(filename) : Qnil;
}

/* call-seq: transaction_active?
 *
 * Returns true if a transaction is currently in progress.
 */
VALUE Database_transaction_active_p(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return sqlite3_get_autocommit(db->sqlite3_db) ? Qfalse : Qtrue;
}

/* call-seq: load_extension(path)
 *
 * Loads an extension with the given path.
 */
VALUE Database_load_extension(VALUE self, VALUE path) {
  Database_t *db;
  GetOpenDatabase(self, db);
  char *err_msg;

  int rc = sqlite3_load_extension(db->sqlite3_db, RSTRING_PTR(path), 0, &err_msg);
  if (rc != SQLITE_OK) {
    VALUE error = rb_exc_new2(cError, err_msg);
    sqlite3_free(err_msg);
    rb_exc_raise(error);
  }

  return self;
}

void Init_Extralite() {
  VALUE mExtralite = rb_define_module("Extralite");
  rb_define_singleton_method(mExtralite, "sqlite3_version", Extralite_sqlite3_version, 0);

  VALUE cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
  rb_define_alloc_func(cDatabase, Database_allocate);

  rb_define_method(cDatabase, "initialize", Database_initialize, 1);
  rb_define_method(cDatabase, "close", Database_close, 0);
  rb_define_method(cDatabase, "closed?", Database_closed_p, 0);

  rb_define_method(cDatabase, "query", Database_query_hash, -1);
  rb_define_method(cDatabase, "query_hash", Database_query_hash, -1);
  rb_define_method(cDatabase, "query_ary", Database_query_ary, -1);
  rb_define_method(cDatabase, "query_single_row", Database_query_single_row, -1);
  rb_define_method(cDatabase, "query_single_column", Database_query_single_column, -1);
  rb_define_method(cDatabase, "query_single_value", Database_query_single_value, -1);

  rb_define_method(cDatabase, "last_insert_rowid", Database_last_insert_rowid, 0);
  rb_define_method(cDatabase, "changes", Database_changes, 0);
  rb_define_method(cDatabase, "filename", Database_filename, -1);
  rb_define_method(cDatabase, "transaction_active?", Database_transaction_active_p, 0);
  rb_define_method(cDatabase, "load_extension", Database_load_extension, 1);

  cError = rb_define_class_under(mExtralite, "Error", rb_eRuntimeError);
  cSQLError = rb_define_class_under(mExtralite, "SQLError", cError);
  cBusyError = rb_define_class_under(mExtralite, "BusyError", cError);
  rb_gc_register_mark_object(cError);
  rb_gc_register_mark_object(cSQLError);
  rb_gc_register_mark_object(cBusyError);

  ID_KEYS   = rb_intern("keys");
  ID_STRIP  = rb_intern("strip");
  ID_TO_S   = rb_intern("to_s");
}
