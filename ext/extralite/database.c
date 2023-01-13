#include <stdio.h>
#include "extralite.h"

VALUE cDatabase;
VALUE cError;
VALUE cSQLError;
VALUE cBusyError;

ID ID_KEYS;
ID ID_NEW;
ID ID_STRIP;
ID ID_TO_S;

static size_t Database_size(const void *ptr) {
  return sizeof(Database_t);
}

static void Database_free(void *ptr) {
  Database_t *db = ptr;
  if (db->sqlite3_db) sqlite3_close(db->sqlite3_db);
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

sqlite3 *Database_sqlite3_db(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);
  return db->sqlite3_db;
}

/* call-seq:
 *   Extralite.sqlite3_version -> version
 *
 * Returns the sqlite3 version used by Extralite.
 */

VALUE Extralite_sqlite3_version(VALUE self) {
  return rb_str_new_cstr(sqlite3_version);
}

/* call-seq:
 *   db.initialize(path)
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

#ifdef HAVE_SQLITE3_ENABLE_LOAD_EXTENSION
  rc = sqlite3_enable_load_extension(db->sqlite3_db, 1);
  if (rc) {
    sqlite3_close(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }
#endif

  return Qnil;
}

/* call-seq:
 *   db.close -> db
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

/* call-seq:
 *   db.closed? -> bool
 *
 * Returns true if the database is closed.
 *
 * @return [bool] is database closed
 */
VALUE Database_closed_p(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);

  return db->sqlite3_db ? Qfalse : Qtrue;
}

static inline VALUE Database_perform_query(int argc, VALUE *argv, VALUE self, VALUE (*call)(query_ctx *)) {
  Database_t *db;
  sqlite3_stmt *stmt;
  VALUE sql;

  // extract query from args
  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  // prepare query ctx
  GetOpenDatabase(self, db);
  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc - 1, argv + 1);
  query_ctx ctx = { self, db->sqlite3_db, stmt };

  return rb_ensure(SAFE(call), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* call-seq:
 *    db.query(sql, *parameters, &block) -> [...]
 *    db.query_hash(sql, *parameters, &block) -> [...]
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
  return Database_perform_query(argc, argv, self, safe_query_hash);
}

/* call-seq:
 *   db.query_ary(sql, *parameters, &block) -> [...]
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
  return Database_perform_query(argc, argv, self, safe_query_ary);
}

/* call-seq:
 *   db.query_single_row(sql, *parameters) -> {...}
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
  return Database_perform_query(argc, argv, self, safe_query_single_row);
}

/* call-seq:
 *   db.query_single_column(sql, *parameters, &block) -> [...]
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
  return Database_perform_query(argc, argv, self, safe_query_single_column);
}

/* call-seq:
 *   db.query_single_value(sql, *parameters) -> value
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
  return Database_perform_query(argc, argv, self, safe_query_single_value);
}

/* call-seq:
 *   db.execute_multi(sql, params_array) -> changes
 *
 * Executes the given query for each list of parameters in params_array. Returns
 * the number of changes effected. This method is designed for inserting
 * multiple records.
 *
 *     records = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     db.execute_multi_query('insert into foo values (?, ?, ?)', records)
 *
 */
VALUE Database_execute_multi(VALUE self, VALUE sql, VALUE params_array) {
  Database_t *db;
  sqlite3_stmt *stmt;

  if (RSTRING_LEN(sql) == 0) return Qnil;

  // prepare query ctx
  GetOpenDatabase(self, db);
  prepare_single_stmt(db->sqlite3_db, &stmt, sql);
  query_ctx ctx = { self, db->sqlite3_db, stmt, params_array };

  return rb_ensure(SAFE(safe_execute_multi), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* call-seq:
 *   db.columns(sql) -> columns
 *
 * Returns the column names for the given query, without running it.
 */
VALUE Database_columns(VALUE self, VALUE sql) {
  return Database_perform_query(1, &sql, self, safe_query_columns);
}

/* call-seq:
 *   db.last_insert_rowid -> int
 *
 * Returns the rowid of the last inserted row.
 */
VALUE Database_last_insert_rowid(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return INT2FIX(sqlite3_last_insert_rowid(db->sqlite3_db));
}

/* call-seq:
 *   db.changes -> int
 *
 * Returns the number of changes made to the database by the last operation.
 */
VALUE Database_changes(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return INT2FIX(sqlite3_changes(db->sqlite3_db));
}

/* call-seq:
 *   db.filename -> string
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

/* call-seq:
 *   db.transaction_active? -> bool
 *
 * Returns true if a transaction is currently in progress.
 */
VALUE Database_transaction_active_p(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  return sqlite3_get_autocommit(db->sqlite3_db) ? Qfalse : Qtrue;
}

#ifdef HAVE_SQLITE3_LOAD_EXTENSION
/* call-seq:
 *   db.load_extension(path) -> db
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
#endif

/* call-seq:
 *   db.prepare(sql) -> Extralite::PreparedStatement
 *
 * Creates a prepared statement with the given SQL query.
 */
VALUE Database_prepare(VALUE self, VALUE sql) {
  return rb_funcall(cPreparedStatement, ID_NEW, 2, self, sql);
}

/* call-seq:
 *   db.interrupt
 *
 * This function causes any pending database operation to abort
 * and return at its earliest opportunity.
 *
 * It is safe to call this routine from a thread different from
 * the thread that is currently running the database operation.
 * But it is not safe to call this routine with a database connection
 * that is closed or might close before sqlite3_interrupt() returns.
 *
 * If an SQL operation is very nearly finished at the time
 * when sqlite3_interrupt() is called, then it might not have
 * an opportunity to be interrupted and might continue to completion.
 *
 * An SQL operation that is interrupted will return SQLITE_INTERRUPT.
 * If the interrupted SQL operation is an INSERT, UPDATE, or DELETE
 * that is inside an explicit transaction, then the entire transaction
 * will be rolled back automatically.
 */
VALUE Database_interrupt(VALUE self) {
  Database_t *db;
  GetOpenDatabase(self, db);

  sqlite3_interrupt(db->sqlite3_db);
  return self;
}

void Init_ExtraliteDatabase() {
  VALUE mExtralite = rb_define_module("Extralite");
  rb_define_singleton_method(mExtralite, "sqlite3_version", Extralite_sqlite3_version, 0);

  cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
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
  rb_define_method(cDatabase, "execute_multi", Database_execute_multi, 2);
  rb_define_method(cDatabase, "columns", Database_columns, 1);

  rb_define_method(cDatabase, "last_insert_rowid", Database_last_insert_rowid, 0);
  rb_define_method(cDatabase, "changes", Database_changes, 0);
  rb_define_method(cDatabase, "filename", Database_filename, -1);
  rb_define_method(cDatabase, "transaction_active?", Database_transaction_active_p, 0);

#ifdef HAVE_SQLITE3_LOAD_EXTENSION
  rb_define_method(cDatabase, "load_extension", Database_load_extension, 1);
#endif

  rb_define_method(cDatabase, "prepare", Database_prepare, 1);
  rb_define_method(cDatabase, "interrupt", Database_interrupt, 0);

  cError = rb_define_class_under(mExtralite, "Error", rb_eRuntimeError);
  cSQLError = rb_define_class_under(mExtralite, "SQLError", cError);
  cBusyError = rb_define_class_under(mExtralite, "BusyError", cError);
  rb_gc_register_mark_object(cError);
  rb_gc_register_mark_object(cSQLError);
  rb_gc_register_mark_object(cBusyError);

  ID_KEYS   = rb_intern("keys");
  ID_NEW    = rb_intern("new");
  ID_STRIP  = rb_intern("strip");
  ID_TO_S   = rb_intern("to_s");
}
