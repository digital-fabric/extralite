#include <stdio.h>
#include <stdlib.h>
#include "extralite.h"

VALUE cDatabase;
VALUE cBlob;
VALUE cError;
VALUE cSQLError;
VALUE cBusyError;
VALUE cInterruptError;
VALUE cParameterError;
VALUE eArgumentError;

ID ID_bind;
ID ID_call;
ID ID_each;
ID ID_keys;
ID ID_new;
ID ID_pragma;
ID ID_strip;
ID ID_to_s;
ID ID_track;

VALUE SYM_at_least_once;
VALUE SYM_gvl_release_threshold;
VALUE SYM_once;
VALUE SYM_none;
VALUE SYM_normal;
VALUE SYM_pragma;
VALUE SYM_read_only;
VALUE SYM_wal;

struct progress_handler global_progress_handler = {
  .mode       = PROGRESS_NONE,
  .proc       = Qnil,
  .period     = DEFAULT_PROGRESS_HANDLER_PERIOD,
  .tick       = DEFAULT_PROGRESS_HANDLER_TICK,
  .tick_count = 0,
  .call_count = 0
};

#define DB_GVL_MODE(db) Database_prepare_gvl_mode(db)

static size_t Database_size(const void *ptr) {
  return sizeof(Database_t);
}

static void Database_mark(void *ptr) {
  Database_t *db = ptr;
  rb_gc_mark_movable(db->trace_proc);
  rb_gc_mark_movable(db->progress_handler.proc);
}

static void Database_compact(void *ptr) {
  Database_t *db = ptr;
  db->trace_proc            = rb_gc_location(db->trace_proc);
  db->progress_handler.proc = rb_gc_location(db->progress_handler.proc);
}

static void Database_free(void *ptr) {
  Database_t *db = ptr;
  if (db->sqlite3_db) sqlite3_close_v2(db->sqlite3_db);
  free(ptr);
}

static const rb_data_type_t Database_type = {
    "Database",
    {Database_mark, Database_free, Database_size, Database_compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static VALUE Database_allocate(VALUE klass) {
  Database_t *db = ALLOC(Database_t);
  db->sqlite3_db = NULL;
  db->trace_proc = Qnil;
  db->progress_handler.proc = Qnil;
  db->progress_handler.mode = PROGRESS_NONE;
  return TypedData_Wrap_Struct(klass, &Database_type, db);
}

inline Database_t *self_to_database(VALUE self) {
  Database_t *db;
  TypedData_Get_Struct(self, Database_t, &Database_type, db);
  return db;
}

inline Database_t *self_to_open_database(VALUE self) {
  Database_t *db = self_to_database(self);
  if (!(db)->sqlite3_db) rb_raise(cError, "Database is closed");

  return db;
}

inline sqlite3 *Database_sqlite3_db(VALUE self) {
  return self_to_database(self)->sqlite3_db;
}

/* Returns the sqlite3 library version used by Extralite.
 *
 * @return [String] SQLite version
 */
VALUE Extralite_sqlite3_version(VALUE self) {
  return rb_str_new_cstr(sqlite3_version);
}

static inline int db_open_flags_from_opts(VALUE opts) {
  if (opts == Qnil) goto default_flags;

  if (TYPE(opts) != T_HASH)
    rb_raise(eArgumentError, "Expected hash as database initialization options");

  VALUE read_only = rb_hash_aref(opts, SYM_read_only);
  if (RTEST(read_only)) return SQLITE_OPEN_READONLY;
default_flags:
  return SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
}

void Database_apply_opts(VALUE self, Database_t *db, VALUE opts) {
  VALUE value = Qnil;

  // :gvl_release_threshold
  value = rb_hash_aref(opts, SYM_gvl_release_threshold);
  if (!NIL_P(value)) db->gvl_release_threshold = NUM2INT(value);

  // :pragma
  value = rb_hash_aref(opts, SYM_pragma);
  if (!NIL_P(value)) rb_funcall(self, ID_pragma, 1, value);

  // :wal
  value = rb_hash_aref(opts, SYM_wal);
  if (RTEST(value)) {
    int rc = sqlite3_exec(db->sqlite3_db, "PRAGMA journal_mode=wal", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
      rb_raise(cError, "Failed to set WAL journaling mode: %s", sqlite3_errstr(rc));
    rc = sqlite3_exec(db->sqlite3_db, "PRAGMA synchronous=1", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
      rb_raise(cError, "Failed to set synchronous mode: %s", sqlite3_errstr(rc));
  }
}

int Database_progress_handler(void *ptr) {
  Database_t *db = (Database_t *)ptr;
  db->progress_handler.tick_count += db->progress_handler.tick;
  if (db->progress_handler.tick_count < db->progress_handler.period)
    goto done;

  db->progress_handler.tick_count -= db->progress_handler.period;
  db->progress_handler.call_count += 1;
  rb_funcall(db->progress_handler.proc, ID_call, 0);
done:
  return 0;
}

int Database_busy_handler(void *ptr, int v) {
  Database_t *db = (Database_t *)ptr;
  rb_funcall(db->progress_handler.proc, ID_call, 1, Qtrue);
  return 1;
}

/* Initializes a new SQLite database with the given path and options:
 *
 * - `:gvl_release_threshold` (`Integer`): sets the GVL release threshold (see
 *   `#gvl_release_threshold=`).
 * - `:pragma` (`Hash`): one or more pragmas to set upon opening the database.
 * - `:read_only` (`true`/`false`): opens the database in read-only mode if true.
 * - `:wal` (`true`/`false`): sets up the database for [WAL journaling
 *   mode](https://www.sqlite.org/wal.html) by setting `PRAGMA journal_mode=wal`
 *   and `PRAGMA synchronous=1`.
 *
 * @overload initialize(path)
 *   @param path [String] file path (or ':memory:' for memory database)
 *   @return [void]
 * @overload initialize(path, gvl_release_threshold: , on_progress: , read_only: , wal: )
 *   @param path [String] file path (or ':memory:' for memory database)
 *   @param options [Hash] options for opening the database
 *   @return [void]
 */
VALUE Database_initialize(int argc, VALUE *argv, VALUE self) {
  Database_t *db = self_to_database(self);
  VALUE path;
  VALUE opts = Qnil;

  rb_scan_args(argc, argv, "11", &path, &opts);
  int flags = db_open_flags_from_opts(opts);

  int rc = sqlite3_open_v2(StringValueCStr(path), &db->sqlite3_db, flags, NULL);
  if (rc) {
    sqlite3_close_v2(db->sqlite3_db);
    db->sqlite3_db = NULL;
    rb_raise(cError, "%s", sqlite3_errstr(rc));
  }

  // Enable extended result codes
  rc = sqlite3_extended_result_codes(db->sqlite3_db, 1);
  if (rc) {
    sqlite3_close_v2(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

#ifdef HAVE_SQLITE3_ENABLE_LOAD_EXTENSION
  rc = sqlite3_enable_load_extension(db->sqlite3_db, 1);
  if (rc) {
    sqlite3_close_v2(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }
#endif

  db->trace_proc = Qnil;
  db->gvl_release_threshold = DEFAULT_GVL_RELEASE_THRESHOLD;

  db->progress_handler = global_progress_handler;
  db->progress_handler.tick_count = 0;
  db->progress_handler.call_count = 0;
  if (db->progress_handler.mode != PROGRESS_NONE) {
      db->gvl_release_threshold = -1;
    if (db->progress_handler.mode != PROGRESS_ONCE)
      sqlite3_progress_handler(db->sqlite3_db, db->progress_handler.tick, &Database_progress_handler, db);
    sqlite3_busy_handler(db->sqlite3_db, &Database_busy_handler, db);
  }

  if (!NIL_P(opts)) Database_apply_opts(self, db, opts);
  return Qnil;
}

/* Returns true if the database was open for read only access.
 *
 * @return [boolean] true if database is open for read only access
 */
VALUE Database_read_only_p(VALUE self) {
  Database_t *db = self_to_database(self);
  int open = sqlite3_db_readonly(db->sqlite3_db, "main");
  return (open == 1) ? Qtrue : Qfalse;
}

/* Closes the database.
 * 
 * @return [Extralite::Database] database
 */
VALUE Database_close(VALUE self) {
  int rc;
  Database_t *db = self_to_database(self);

  rc = sqlite3_close_v2(db->sqlite3_db);
  if (rc) {
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  db->sqlite3_db = NULL;
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
  Database_t *db = self_to_database(self);
  return db->sqlite3_db ? Qfalse : Qtrue;
}

inline enum gvl_mode Database_prepare_gvl_mode(Database_t *db) {
  return db->gvl_release_threshold < 0 ? GVL_HOLD : GVL_RELEASE;
}

static inline VALUE Database_perform_query(int argc, VALUE *argv, VALUE self, VALUE (*call)(query_ctx *), enum query_mode query_mode) {
  Database_t *db = self_to_open_database(self);
  sqlite3_stmt *stmt;
  VALUE sql = Qnil;
  VALUE transform = Qnil;
  // transform mode is set and the first parameter is not a string, so we expect
  // a transform.
  int got_transform = (TYPE(argv[0]) != T_STRING);
  
  // extract query from args
  rb_check_arity(argc, got_transform ? 2 : 1, UNLIMITED_ARGUMENTS);

  if (got_transform) {
    transform = argv[0];
    argc--;
    argv++;
  }

  sql = rb_funcall(argv[0], ID_strip, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  Database_issue_query(db, sql);
  prepare_multi_stmt(DB_GVL_MODE(db), db->sqlite3_db, &stmt, sql);
  RB_GC_GUARD(sql);

  bind_all_parameters(stmt, argc - 1, argv + 1);
  query_ctx ctx = QUERY_CTX(
    self, sql, db, stmt, Qnil, transform,
    query_mode, ROW_YIELD_OR_MODE(ROW_MULTI), ALL_ROWS
  );

  VALUE result = rb_ensure(SAFE(call), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
  RB_GC_GUARD(result);
  return result;
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
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     db.query('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using keyword arguments:
 *
 *     db.query('select * from foo where x = :bar', bar: 42)
 * 
 * @overload query(sql, ...)
 *   @param sql [String] SQL statement
 *   @return [Array<Hash>, Integer] rows or total changes
 * @overload query(transform, sql, ...)
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array<Hash>, Integer] rows or total changes
 */
VALUE Database_query(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_hash, QUERY_HASH);
}

/* Runs a query and transforms rows through the given transform poc. Each row is
 * provided to the transform proc as a list of values. If a block is given, it
 * will be called for each row. Otherwise, an array containing all rows is
 * returned.
 *
 * If a transform block is given, it is called for each row, with the row values
 * splatted:
 *
 *     transform = ->(a, b, c) { a * 100 + b * 10 + c }
 *     db.query_splat(transform, 'select a, b, c from foo where c = ?', 42)
 *
 * @overload query_splat(sql, ...)
 *   @param sql [String] SQL statement
 *   @return [Array<Array, any>, Integer] rows or total changes
 * @overload query_splat(transform, sql, ...)
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array<Array, any>, Integer] rows or total changes
 */
VALUE Database_query_splat(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_splat, QUERY_SPLAT);
}

/* Runs a query returning rows as arrays. If a block is given, it will be called
 * for each row. Otherwise, an array containing all rows is returned.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
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
 * 
 * @overload query_ary(sql, ...)
 *   @param sql [String] SQL statement
 *   @return [Array<Array>, Integer] rows or total changes
 * @overload query_ary(transform, sql, ...)
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array<Array>, Integer] rows or total changes
 */
VALUE Database_query_ary(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_ary, QUERY_ARY);
}

/* Runs a query returning a single row as a hash.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     db.query_single('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using keyword arguments:
 *
 *     db.query_single('select * from foo where x = :bar', bar: 42)
 *
 * @overload query_single(sql, ...) -> row
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 * @overload query_single(transform, sql, ...) -> row
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 */
VALUE Database_query_single(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_single_row_hash, QUERY_HASH);
}

/* Runs a query returning a single row as an array or a single value.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     db.query_single_splat('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using keyword arguments:
 *
 *     db.query_single_splat('select * from foo where x = :bar', bar: 42)
 * 
 * @overload query_single_splat(sql, ...) -> row
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 * @overload query_single_splat(transform, sql, ...) -> row
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 */
VALUE Database_query_single_splat(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_single_row_splat, QUERY_SPLAT);
}

/* Runs a query returning a single row as an array.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     db.query_single_ary('select * from foo where x = ?', 42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using keyword arguments:
 *
 *     db.query_single_ary('select * from foo where x = :bar', bar: 42)
 * 
 * @overload query_single_ary(sql, ...) -> row
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 * @overload query_single_ary(transform, sql, ...) -> row
 *   @param transform [Proc] transform proc
 *   @param sql [String] SQL statement
 *   @return [Array, any] row
 */
VALUE Database_query_single_ary(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_single_row_ary, QUERY_ARY);
}

/* call-seq:
 *   db.execute(sql, *parameters) -> changes
 *
 * Runs a query returning the total changes effected. This method should be used
 * for data- or schema-manipulation queries.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     db.execute('update foo set x = ? where y = ?', 42, 43)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using keyword arguments:
 *
 *     db.execute('update foo set x = :bar', bar: 42)
 */
VALUE Database_execute(int argc, VALUE *argv, VALUE self) {
  return Database_perform_query(argc, argv, self, safe_query_changes, QUERY_HASH);
}

/* call-seq:
 *   db.batch_execute(sql, params_source) -> changes
 *
 * Executes the given query for each list of parameters in the paramter source.
 * If an enumerable is given, it is iterated and each of its values is used as
 * the parameters for running the query. If a callable is given, it is called
 * repeatedly and each of its return values is used as the parameters, until nil
 * is returned.
 *
 * Returns the number of changes effected. This method is designed for inserting
 * multiple records or performing other mass operations.
 *
 *     records = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     db.batch_execute('insert into foo values (?, ?, ?)', records)
 *
 *     source = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     db.batch_execute('insert into foo values (?, ?, ?)', -> { records.shift })
 *
 * @param sql [String] query SQL
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] parameters to run query with
 * @return [Integer] Total number of changes effected
 */
VALUE Database_batch_execute(VALUE self, VALUE sql, VALUE parameters) {
  Database_t *db = self_to_open_database(self);
  sqlite3_stmt *stmt;

  if (RSTRING_LEN(sql) == 0) return Qnil;

  prepare_single_stmt(DB_GVL_MODE(db), db->sqlite3_db, &stmt, sql);
  query_ctx ctx = QUERY_CTX(
    self, sql, db, stmt, parameters,
    Qnil, QUERY_HASH, ROW_MULTI, ALL_ROWS
  );

  return rb_ensure(SAFE(safe_batch_execute), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* call-seq:
 *   db.batch_query(sql, params_source) -> rows
 *   db.batch_query(sql, params_source) { |rows| ... } -> changes
 *   db.batch_query_hash(sql, params_source) -> rows
 *   db.batch_query_hash(sql, params_source) { |rows| ... } -> changes
 *
 * Executes the given query for each list of parameters in the given paramter
 * source. If a block is given, it is called with the resulting rows for each
 * invocation of the query, and the total number of changes is returned.
 * Otherwise, an array containing the resulting rows for each invocation is
 * returned.
 *
 *     records = [
 *       [1, 2],
 *       [3, 4]
 *     ]
 *     db.batch_query('insert into foo values (?, ?) returning bar, baz', records)
 *     #=> [{ bar: 1, baz: 2 }, { bar: 3, baz: 4}]
 * *
 * @param sql [String] query SQL
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] parameters to run query with
 * @return [Array<Hash>, Integer] Total number of changes effected
 */
VALUE Database_batch_query(VALUE self, VALUE sql, VALUE parameters) {
  Database_t *db = self_to_open_database(self);
  sqlite3_stmt *stmt;

  prepare_single_stmt(DB_GVL_MODE(db), db->sqlite3_db, &stmt, sql);
  query_ctx ctx = QUERY_CTX(
    self, sql, db, stmt, parameters,
    Qnil, QUERY_HASH, ROW_MULTI, ALL_ROWS
  );

  return rb_ensure(SAFE(safe_batch_query), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* call-seq:
 *   db.batch_query_ary(sql, params_source) -> rows
 *   db.batch_query_ary(sql, params_source) { |rows| ... } -> changes
 *
 * Executes the given query for each list of parameters in the given paramter
 * source. If a block is given, it is called with the resulting rows for each
 * invocation of the query, and the total number of changes is returned.
 * Otherwise, an array containing the resulting rows for each invocation is
 * returned. Rows are represented as arrays.
 *
 *     records = [
 *       [1, 2],
 *       [3, 4]
 *     ]
 *     db.batch_query_ary('insert into foo values (?, ?) returning bar, baz', records)
 *     #=> [[1, 2], [3, 4]]
 * *
 * @param sql [String] query SQL
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] parameters to run query with
 * @return [Array<Array>, Integer] Total number of changes effected
 */
VALUE Database_batch_query_ary(VALUE self, VALUE sql, VALUE parameters) {
  Database_t *db = self_to_open_database(self);
  sqlite3_stmt *stmt;

  prepare_single_stmt(DB_GVL_MODE(db), db->sqlite3_db, &stmt, sql);
  query_ctx ctx = QUERY_CTX(
    self, sql, db, stmt, parameters,
    Qnil, QUERY_ARY, ROW_MULTI, ALL_ROWS
  );

  return rb_ensure(SAFE(safe_batch_query_ary), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* call-seq:
 *   db.batch_query_splat(sql, params_source) -> rows
 *   db.batch_query_splat(sql, params_source) { |rows| ... } -> changes
 *
 * Executes the given query for each list of parameters in the given paramter
 * source. If a block is given, it is called with the resulting rows for each
 * invocation of the query, and the total number of changes is returned.
 * Otherwise, an array containing the resulting rows for each invocation is
 * returned. Rows are single values.
 *
 *     records = [
 *       [1, 2],
 *       [3, 4]
 *     ]
 *     db.batch_query_splat('insert into foo values (?, ?) returning baz', records)
 *     #=> [2, 4]
 * *
 * @param sql [String] query SQL
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] parameters to run query with
 * @return [Array<any>, Integer] Total number of changes effected
 */
VALUE Database_batch_query_splat(VALUE self, VALUE sql, VALUE parameters) {
  Database_t *db = self_to_open_database(self);
  sqlite3_stmt *stmt;

  prepare_single_stmt(DB_GVL_MODE(db), db->sqlite3_db, &stmt, sql);
  query_ctx ctx = QUERY_CTX(
    self, sql, db, stmt, parameters,
    Qnil, QUERY_SPLAT, ROW_MULTI, ALL_ROWS
  );

  return rb_ensure(SAFE(safe_batch_query_splat), (VALUE)&ctx, SAFE(cleanup_stmt), (VALUE)&ctx);
}

/* Returns the column names for the given query, without running it.
 * 
 * @return [Array<String>] column names
 */
VALUE Database_columns(VALUE self, VALUE sql) {
  return Database_perform_query(1, &sql, self, safe_query_columns, QUERY_HASH);
}

/* Returns the rowid of the last inserted row.
 * 
 * @return [Integer] last rowid
 */
VALUE Database_last_insert_rowid(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return INT2FIX(sqlite3_last_insert_rowid(db->sqlite3_db));
}

/* Returns the number of changes made to the database by the last operation.
 * 
 * @return [Integer] number of changes
 */
VALUE Database_changes(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return INT2FIX(sqlite3_changes(db->sqlite3_db));
}

/* Returns the database filename. If db_name is given, returns the filename for
 * the respective attached database.
 * 
 * @overload filename()
 *   @return [String] database filename
 * @overload filename(db_name)
 *   @param db_name [String] attached database name
 *   @return [String] database filename
 */
VALUE Database_filename(int argc, VALUE *argv, VALUE self) {
  const char *db_name;
  const char *filename;
  Database_t *db = self_to_open_database(self);

  rb_check_arity(argc, 0, 1);
  db_name = (argc == 1) ? StringValueCStr(argv[0]) : "main";
  filename = sqlite3_db_filename(db->sqlite3_db, db_name);
  return filename ? rb_str_new_cstr(filename) : Qnil;
}

/* Returns true if a transaction is currently in progress.
 * 
 * @return [bool] is transaction in progress
 */
VALUE Database_transaction_active_p(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return sqlite3_get_autocommit(db->sqlite3_db) ? Qfalse : Qtrue;
}

#ifdef HAVE_SQLITE3_LOAD_EXTENSION
/* Loads an extension with the given path.
 * 
 * @param path [String] extension file path
 * @return [Extralite::Database] database
 */
VALUE Database_load_extension(VALUE self, VALUE path) {
  Database_t *db = self_to_open_database(self);
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

static inline VALUE Database_prepare(int argc, VALUE *argv, VALUE self, VALUE mode) {
  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);

  VALUE args[] = { self, argv[0], mode};
  VALUE query = rb_funcall_passing_block(cQuery, ID_new, 3, args);
  if (argc > 1) rb_funcallv(query, ID_bind, argc - 1, argv + 1);
  RB_GC_GUARD(query);
  return query;
}

/* call-seq:
 *   db.prepare(sql) -> query
 *   db.prepare(sql, *params) -> query
 *   db.prepare(sql, *params) { ... } -> query
 *
 * Creates a prepared query with the given SQL query in hash mode. If query
 * parameters are given, they are bound to the query. If a block is given, it is
 * used as a transform proc.
 * 
 * @param sql [String] SQL statement
 * @param *params [Array<any>] parameters to bind
 * @return [Extralite::Query] prepared query
 */
VALUE Database_prepare_hash(int argc, VALUE *argv, VALUE self) {
  return Database_prepare(argc, argv, self, SYM_hash);
}

/* call-seq:
 *   db.prepare_splat(sql) -> Extralite::Query
 *   db.prepare_splat(sql, *params) -> Extralite::Query
 *   db.prepare_splat(sql, *params) { ... } -> Extralite::Query
 *
 * Creates a prepared query with the given SQL query in argv mode. If query
 * parameters are given, they are bound to the query. If a block is given, it is
 * used as a transform proc.
 * 
 * @param sql [String] SQL statement
 * @param *params [Array<any>] parameters to bind
 * @return [Extralite::Query] prepared query
 */
VALUE Database_prepare_splat(int argc, VALUE *argv, VALUE self) {
  return Database_prepare(argc, argv, self, SYM_splat);
}

/* call-seq:
 *   db.prepare_ary(sql) -> Extralite::Query
 *   db.prepare_ary(sql, *params) -> Extralite::Query
 *   db.prepare_ary(sql, *params) { ... } -> Extralite::Query
 *
 * Creates a prepared query with the given SQL query in ary mode. If query
 * parameters are given, they are bound to the query. If a block is given, it is
 * used as a transform proc.
 * 
 * @param sql [String] SQL statement
 * @param *params [Array<any>] parameters to bind
 * @return [Extralite::Query] prepared query
 */
VALUE Database_prepare_ary(int argc, VALUE *argv, VALUE self) {
  return Database_prepare(argc, argv, self, SYM_ary);
}

/* Interrupts a long running query. This method is to be called from a different
 * thread than the one running the query. Upon calling `#interrupt` the running
 * query will stop and raise an `Extralite::InterruptError` exception.
 *
 * It is not safe to call `#interrupt` on a database that is about to be closed.
 * For more information, consult the [sqlite3 API
 * docs](https://sqlite.org/c3ref/interrupt.html).
 * 
 * @return [Extralite::Database] database
 */
VALUE Database_interrupt(VALUE self) {
  Database_t *db = self_to_open_database(self);

  sqlite3_interrupt(db->sqlite3_db);
  return self;
}

typedef struct {
  sqlite3 *dst;
  int close_dst_on_cleanup;
  sqlite3_backup *backup;
  int block_given;
  int rc;
} backup_ctx;

#define BACKUP_STEP_MAX_PAGES 16
#define BACKUP_SLEEP_MS       100

void *backup_step_impl(void *ptr) {
  backup_ctx *ctx = (backup_ctx *)ptr;
  ctx->rc = sqlite3_backup_step(ctx->backup, BACKUP_STEP_MAX_PAGES);
  return NULL;
}

void *backup_sleep_impl(void *unused) {
  sqlite3_sleep(BACKUP_SLEEP_MS);
  return NULL;
}

VALUE backup_safe_iterate(VALUE ptr) {
  backup_ctx *ctx = (backup_ctx *)ptr;
  int done = 0;

  while (!done) {
    gvl_call(GVL_RELEASE, backup_step_impl, (void *)ctx);
    switch(ctx->rc) {
      case SQLITE_DONE:
        if (ctx->block_given) {
          VALUE total     = INT2FIX(sqlite3_backup_pagecount(ctx->backup));
          rb_yield_values(2, total, total);
        }
        done = 1;
        continue;
      case SQLITE_OK:
        if (ctx->block_given) {
          VALUE remaining = INT2FIX(sqlite3_backup_remaining(ctx->backup));
          VALUE total     = INT2FIX(sqlite3_backup_pagecount(ctx->backup));
          rb_yield_values(2, remaining, total);
        }
        continue;
      case SQLITE_BUSY:
      case SQLITE_LOCKED:
        gvl_call(GVL_RELEASE, backup_sleep_impl, NULL);
        continue;
      default:
        rb_raise(cError, "%s", sqlite3_errstr(ctx->rc));
    }
  };

  return Qnil;
}

VALUE backup_cleanup(VALUE ptr) {
  backup_ctx *ctx = (backup_ctx *)ptr;

  sqlite3_backup_finish(ctx->backup);

  if (ctx->close_dst_on_cleanup)
    sqlite3_close_v2(ctx->dst);
  return Qnil;
}

/* Creates a backup of the database to the given destination, which can be
 * either a filename or a database instance. In order to monitor the backup
 * progress you can pass a block that will be called periodically by the backup
 * method with two arguments: the remaining page count, and the total page
 * count, which can be used to display the progress to the user or to collect
 * statistics.
 * 
 *     db_src.backup(db_dest) do |remaining, total|
 *       puts "Backing up #{remaining}/#{total}"
 *     end
 * 
 * @param dest [String, Extralite::Database] backup destination
 * @param src_db_name [String] source database name (default: "main")
 * @param dst_db_name [String] Destination database name (default: "main")
 * @yieldparam remaining [Integer] remaining page count
 * @yieldparam total [Integer] total page count
 * @return [Extralite::Database] source database
 */
VALUE Database_backup(int argc, VALUE *argv, VALUE self) {
  VALUE dst;
  VALUE src_name;
  VALUE dst_name;
  rb_scan_args(argc, argv, "12", &dst, &src_name, &dst_name);
  if (src_name == Qnil) src_name = rb_str_new_literal("main");
  if (dst_name == Qnil) dst_name = rb_str_new_literal("main");

  int dst_is_fn = TYPE(dst) == T_STRING;

  Database_t *src = self_to_open_database(self);
  sqlite3 *dst_db;

  if (dst_is_fn) {
    int rc = sqlite3_open(StringValueCStr(dst), &dst_db);
    if (rc) {
      sqlite3_close_v2(dst_db);
      rb_raise(cError, "%s", sqlite3_errmsg(dst_db));
    }
  }
  else {
    Database_t *dst_struct = self_to_open_database(dst);
    dst_db = dst_struct->sqlite3_db;
  }

  // TODO: add possibility to use different src and dest db names (main, tmp, or
  // attached db's).
  sqlite3_backup *backup;
  backup = sqlite3_backup_init(dst_db, StringValueCStr(dst_name), src->sqlite3_db, StringValueCStr(src_name));
  if (!backup) {
    if (dst_is_fn)
      sqlite3_close_v2(dst_db);
    rb_raise(cError, "%s", sqlite3_errmsg(dst_db));
  }

  backup_ctx ctx = { dst_db, dst_is_fn, backup, rb_block_given_p(), 0 };
  rb_ensure(SAFE(backup_safe_iterate), (VALUE)&ctx, SAFE(backup_cleanup), (VALUE)&ctx);

  RB_GC_GUARD(src_name);
  RB_GC_GUARD(dst_name);

  return self;
}

/* Returns runtime status values for the given op as an array containing the
 * current value and the high water mark value. To reset the high water mark,
 * pass true as reset.
 *
 * @overload runtime_status(op)
 *   @param op [Integer] op
 *   @return [Array<Integer>] array containing the value and high water mark
 * @overload runtime_status(op, reset)
 *   @param op [Integer] op
 *   @param reset [Integer, bool] reset flag
 *   @return [Array<Integer>] array containing the value and high water mark
 */
VALUE Extralite_runtime_status(int argc, VALUE* argv, VALUE self) {
  VALUE op, reset;
  sqlite3_int64 cur, hwm;

  rb_scan_args(argc, argv, "11", &op, &reset);

  int rc = sqlite3_status64(NUM2INT(op), &cur, &hwm, RTEST(reset) ? 1 : 0);
  if (rc != SQLITE_OK) rb_raise(cError, "%s", sqlite3_errstr(rc));

  return rb_ary_new3(2, LONG2FIX(cur), LONG2FIX(hwm));
}

/* Returns database status values for the given op as an array containing the
 * current value and the high water mark value. To reset the high water mark,
 * pass true as reset.
 * 
 * @overload status(op)
 *   @param op [Integer] op
 *   @return [Array<Integer>] array containing the value and high water mark
 * @overload status(op, reset)
 *   @param op [Integer] op
 *   @param reset [Integer, bool] reset flag
 *   @return [Array<Integer>] array containing the value and high water mark
 */
VALUE Database_status(int argc, VALUE *argv, VALUE self) {
  VALUE op, reset;
  int cur, hwm;

  rb_scan_args(argc, argv, "11", &op, &reset);

  Database_t *db = self_to_open_database(self);

  int rc = sqlite3_db_status(db->sqlite3_db, NUM2INT(op), &cur, &hwm, RTEST(reset) ? 1 : 0);
  if (rc != SQLITE_OK) rb_raise(cError, "%s", sqlite3_errstr(rc));

  return rb_ary_new3(2, INT2NUM(cur), INT2NUM(hwm));
}

/* Returns the current limit for the given category. If a new value is given,
 * sets the limit to the new value and returns the previous value.
 * 
 * @overload limit(category)
 *   @param category [Integer] category
 *   @return [Integer] limit value
 * @overload limit(category, new_value)
 *   @param category [Integer] category
 *   @param new_value [Integer] new value
 *   @return [Integer] old value
 */
VALUE Database_limit(int argc, VALUE *argv, VALUE self) {
  VALUE category, new_value;

  rb_scan_args(argc, argv, "11", &category, &new_value);

  Database_t *db = self_to_open_database(self);

  int value = sqlite3_limit(db->sqlite3_db, NUM2INT(category), RTEST(new_value) ? NUM2INT(new_value) : -1);

  if (value == -1) rb_raise(cError, "Invalid limit category");

  return INT2NUM(value);
}

/* Sets the busy timeout for the database, in seconds or fractions thereof. To
 * disable the busy timeout, set it to 0 or nil. When the busy timeout is set to
 * a value larger than zero, running a query when the database is locked will
 * cause the program to wait for the database to become available. If the
 * database is still locked when the timeout period has elapsed, the query will
 * fail with a `Extralite::BusyError` exception.
 * 
 * Setting the busy timeout allows other threads to run while waiting for the
 * database to become available. See also `#on_progress`.
 *
 * @param sec [Number, nil] timeout value
 * @return [Extralite::Database] database
 */
VALUE Database_busy_timeout_set(VALUE self, VALUE sec) {
  Database_t *db = self_to_open_database(self);

  int ms = (sec == Qnil) ? 0 : (int)(NUM2DBL(sec) * 1000);
  int rc = sqlite3_busy_timeout(db->sqlite3_db, ms);
  if (rc != SQLITE_OK) rb_raise(cError, "Failed to set busy timeout");

  return self;
}

/* Returns the total number of changes made to the database since opening it.
 * 
 * @return [Integer] total changes
 */
VALUE Database_total_changes(VALUE self) {
  Database_t *db = self_to_open_database(self);

  int value = sqlite3_total_changes(db->sqlite3_db);
  return INT2NUM(value);
}

/* Installs or removes a block that will be invoked for every SQL statement
 * executed. To stop tracing, call `#trace` without a block.
 * 
 * @return [Extralite::Database] database
 */
VALUE Database_trace(VALUE self) {
  Database_t *db = self_to_open_database(self);

  RB_OBJ_WRITE(self, &db->trace_proc, rb_block_given_p() ? rb_block_proc() : Qnil);
  return self;
}

#ifdef EXTRALITE_ENABLE_CHANGESET
/* call-seq:
 *   db.track_changes(*tables) { ... } -> changeset
 *
 * Tracks changes to the database and returns a changeset. The changeset can
 * then be used to store the changes to a file, apply them to another database,
 * or undo the changes. The given table names specify which tables should be
 * tracked for changes. Passing a value of nil causes all tables to be tracked.
 * 
 *     changeset = db.track_changes(:foo, :bar) do
 *       perform_a_bunch_of_queries
 *     end
 * 
 *     File.open('my.changes', 'w+') { |f| f << changeset.to_blob }
 * 
 * @param *tables [Array<String, Symbol>] table(s) to track
 * @return [Extralite::Changeset] changeset
*/
VALUE Database_track_changes(int argc, VALUE *argv, VALUE self) {
  self_to_open_database(self);

  VALUE changeset = rb_funcall(cChangeset, ID_new, 0);
  VALUE tables = rb_ary_new_from_values(argc, argv);

  rb_funcall(changeset, ID_track, 2, self, tables);

  RB_GC_GUARD(changeset);
  RB_GC_GUARD(tables);
  return changeset;
}
#endif

void Database_reset_progress_handler(VALUE self, Database_t *db) {
  db->progress_handler.mode = PROGRESS_NONE;
  RB_OBJ_WRITE(self, &db->progress_handler.proc, Qnil);
  sqlite3_progress_handler(db->sqlite3_db, 0, NULL, NULL);
  sqlite3_busy_handler(db->sqlite3_db, NULL, NULL);
}

static inline enum progress_handler_mode symbol_to_progress_mode(VALUE mode) {
  if (mode == SYM_at_least_once)  return PROGRESS_AT_LEAST_ONCE;
  if (mode == SYM_once)           return PROGRESS_ONCE;
  if (mode == SYM_normal)         return PROGRESS_NORMAL;
  if (mode == SYM_none)           return PROGRESS_NONE;
  rb_raise(eArgumentError, "Invalid progress handler mode");
}

inline void Database_issue_query(Database_t *db, VALUE sql) {
  if (db->trace_proc != Qnil) rb_funcall(db->trace_proc, ID_call, 1, sql);
  switch (db->progress_handler.mode) {
    case PROGRESS_AT_LEAST_ONCE:
    case PROGRESS_ONCE:
      rb_funcall(db->progress_handler.proc, ID_call, 0);
    default:
      ; // do nothing

  }
}

struct progress_handler parse_progress_handler_opts(VALUE opts) {
  static ID kw_ids[3];
  VALUE kw_args[3];
  struct progress_handler prog = {
    .mode   = rb_block_given_p() ? PROGRESS_NORMAL : PROGRESS_NONE,
    .proc   = rb_block_given_p() ? rb_block_proc() : Qnil,
    .period = DEFAULT_PROGRESS_HANDLER_PERIOD,
    .tick   = DEFAULT_PROGRESS_HANDLER_TICK
  };

  if (!NIL_P(opts)) {
    if (!kw_ids[0]) {
      CONST_ID(kw_ids[0], "period");
      CONST_ID(kw_ids[1], "tick");
      CONST_ID(kw_ids[2], "mode");
    }

    rb_get_kwargs(opts, kw_ids, 0, 3, kw_args);
    if (kw_args[0] != Qundef) { prog.period = NUM2INT(kw_args[0]); }
    if (kw_args[1] != Qundef) { prog.tick   = NUM2INT(kw_args[1]); }
    if (kw_args[2] != Qundef) { prog.mode   = symbol_to_progress_mode(kw_args[2]); }
    if (prog.tick > prog.period) prog.tick = prog.period;
  }
  if (NIL_P(prog.proc) || (prog.period <= 0)) prog.mode = PROGRESS_NONE;
  if (prog.mode == PROGRESS_NONE) prog.proc = Qnil;

  return prog;
}

/* Installs or removes a progress handler that will be executed periodically
 * while a query is running. This method can be used to support switching
 * between fibers and threads or implementing timeouts for running queries.
 *
 * The `period` parameter specifies the approximate number of SQLite
 * virtual machine instructions that are evaluated between successive
 * invocations of the progress handler. A period of less than 1 removes the
 * progress handler. The default period value is 1000.
 *
 * The optional `tick` parameter specifies the granularity of how often the
 * progress handler is called. The default tick value is 10, which means that
 * Extralite's underlying progress callback will be called every 10 SQLite VM
 * instructions. The given progress proc, however, will be only called every
 * `period` (cumulative) VM instructions. This allows the progress handler to
 * work correctly also when running simple queries that don't include many
 * VM instructions. If the `tick` value is greater than the period value it is
 * automatically capped to the period value.
 * 
 * The `mode` parameter controls the progress handler mode, which is one of the
 * following:
 * 
 * - `:normal` (default): the progress handler proc is invoked on query
 *   progress.
 * - `:once`: the progress handler proc is invoked only once, when preparing the
 *   query.
 * - `:at_least_once`: the progress handler proc is invoked when prearing the
 *   query, and on query progress.
 *
 * The progress handler is called also when the database is busy. This lets the
 * application perform work while waiting for the database to become unlocked,
 * or implement a timeout. Note that setting the database's busy_timeout _after_
 * setting a progress handler may lead to undefined behaviour in a concurrent
 * application. When busy, the progress handler proc is passed `true` as the
 * first argument.
 *
 * When the progress handler is set, the gvl release threshold value is set to
 * -1, which means that the GVL will not be released at all when preparing or
 * running queries. It is the application's responsibility to let other threads
 * or fibers run by calling e.g. Thread.pass:
 *
 *     db.on_progress do
 *       do_something_interesting
 *       Thread.pass # let other threads run
 *     end
 *
 * Note that the progress handler is set globally for the database and that
 * Extralite does provide any hooks for telling which queries are currently
 * running or at what time they were started. This means that you'll need to
 * wrap the stock #query_xxx and #execute methods with your own code that
 * calculates timeouts, for example:
 *
 *     def setup_progress_handler
 *       @db.on_progress do
 *         raise TimeoutError if Time.now - @t0 >= @timeout
 *         Thread.pass
 *       end
 *     end
 *
 *     def query(sql, *)
 *       @t0 = Time.now
 *       @db.query(sql, *)
 *     end
 *
 * If the gvl release threshold is set to a value equal to or larger than 0
 * after setting the progress handler, the progress handler will be reset.
 *
 * @param period [Integer] progress handler period
 * @param [Hash] opts progress options
 * @option opts [Integer] :period period value (`1000` by default)
 * @option opts [Integer] :tick tick value (`10` by default)
 * @option opts [Symbol] :mode progress handler mode (`:normal` by default)
 * @return [Extralite::Database] database
 */
VALUE Database_on_progress(int argc, VALUE *argv, VALUE self) {
  Database_t *db = self_to_open_database(self);
  VALUE opts;
  struct progress_handler prog;

  rb_scan_args(argc, argv, "00:", &opts);
  prog = parse_progress_handler_opts(opts);

  if (prog.mode == PROGRESS_NONE) {
    Database_reset_progress_handler(self, db);
    db->gvl_release_threshold = DEFAULT_GVL_RELEASE_THRESHOLD;
    return self;
  }

  db->gvl_release_threshold = -1;
  db->progress_handler.mode       = prog.mode;
  RB_OBJ_WRITE(self, &db->progress_handler.proc, prog.proc);
  db->progress_handler.period     = prog.period;
  db->progress_handler.tick       = prog.tick;
  db->progress_handler.tick_count = 0;
  db->progress_handler.call_count = 0;

  // The PROGRESS_ONCE mode works by invoking the progress handler proc exactly
  // once, before iterating over the result set, so in that mode we don't
  // actually need to set the progress handler at the sqlite level.
  if (prog.mode != PROGRESS_ONCE)
    sqlite3_progress_handler(db->sqlite3_db, prog.tick, &Database_progress_handler, db);
  if (prog.mode != PROGRESS_NONE)
    sqlite3_busy_handler(db->sqlite3_db, &Database_busy_handler, db);

  return self;
}

/* call-seq:
 *   Extralite.on_progress(**opts) { ... }
 * 
 * Installs or removes a global progress handler that will be executed
 * periodically while a query is running. This method can be used to support
 * switching between fibers and threads or implementing timeouts for running
 * queries.
 * 
 * This method sets the progress handler settings and behaviour for all
 * subsequently created `Database` instances. Calling this method will have no
 * effect on already existing `Database` instances
 *
 * The `period` parameter specifies the approximate number of SQLite
 * virtual machine instructions that are evaluated between successive
 * invocations of the progress handler. A period of less than 1 removes the
 * progress handler. The default period value is 1000.
 *
 * The optional `tick` parameter specifies the granularity of how often the
 * progress handler is called. The default tick value is 10, which means that
 * Extralite's underlying progress callback will be called every 10 SQLite VM
 * instructions. The given progress proc, however, will be only called every
 * `period` (cumulative) VM instructions. This allows the progress handler to
 * work correctly also when running simple queries that don't include many
 * VM instructions. If the `tick` value is greater than the period value it is
 * automatically capped to the period value.
 * 
 * The `mode` parameter controls the progress handler mode, which is one of the
 * following:
 * 
 * - `:normal` (default): the progress handler proc is invoked on query
 *   progress.
 * - `:once`: the progress handler proc is invoked only once, when preparing the
 *   query.
 * - `:at_least_once`: the progress handler proc is invoked when prearing the
 *   query, and on query progress.
 *
 * The progress handler is called also when the database is busy. This lets the
 * application perform work while waiting for the database to become unlocked,
 * or implement a timeout. Note that setting the database's busy_timeout _after_
 * setting a progress handler may lead to undefined behaviour in a concurrent
 * application. When busy, the progress handler proc is passed `true` as the
 * first argument.
 *
 * When the progress handler is set, the gvl release threshold value is set to
 * -1, which means that the GVL will not be released at all when preparing or
 * running queries. It is the application's responsibility to let other threads
 * or fibers run by calling e.g. Thread.pass:
 *
 *     Extralite.on_progress do
 *       do_something_interesting
 *       Thread.pass # let other threads run
 *     end
 *
 * @param period [Integer] progress handler period
 * @param [Hash] opts progress options
 * @option opts [Integer] :period period value (`1000` by default)
 * @option opts [Integer] :tick tick value (`10` by default)
 * @option opts [Symbol] :mode progress handler mode (`:normal` by default)
 * @return [Extralite::Database] database
 */
VALUE Extralite_on_progress(int argc, VALUE *argv, VALUE self) {
  VALUE opts;

  rb_scan_args(argc, argv, "00:", &opts);
  global_progress_handler = parse_progress_handler_opts(opts);
  return self;
}

/* Returns the last error code for the database.
 * 
 * @return [Integer] last error code
 */
VALUE Database_errcode(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return INT2NUM(sqlite3_errcode(db->sqlite3_db));
}

/* Returns the last error message for the database.
 * 
 * @return [String] last error message
 */
VALUE Database_errmsg(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return rb_str_new2(sqlite3_errmsg(db->sqlite3_db));
}

#ifdef HAVE_SQLITE3_ERROR_OFFSET
/* Returns the offset for the last error. This is useful for indicating where in
 * the SQL string an error was encountered.
 * 
 * @return [Integer] offset in the last submitted SQL string
 */
VALUE Database_error_offset(VALUE self) {
  Database_t *db = self_to_open_database(self);

  return INT2NUM(sqlite3_error_offset(db->sqlite3_db));
}
#endif

/* Returns a short string representation of the database instance, including the
 * database filename.
 *
 * @return [String] string representation
 */
VALUE Database_inspect(VALUE self) {
  Database_t *db = self_to_database(self);
  VALUE cname = rb_class_name(CLASS_OF(self));

  if (!(db)->sqlite3_db)
    return rb_sprintf("#<%"PRIsVALUE":%p (closed)>", cname, (void*)self);
  else {
    VALUE filename = Database_filename(0, NULL, self);
    if (RSTRING_LEN(filename) == 0) filename = rb_str_new_literal(":memory:");
    return rb_sprintf("#<%"PRIsVALUE":%p %"PRIsVALUE">", cname, (void*)self, filename);
  }
}

/* Returns the database's GVL release threshold.
 *
 * @return [Integer] GVL release threshold
 */
VALUE Database_gvl_release_threshold_get(VALUE self) {
  Database_t *db = self_to_open_database(self);
  return INT2NUM(db->gvl_release_threshold);
}

/* Sets the database's GVL release threshold. The release policy changes
 * according to the given value:
 * 
 * - Less than 0: the GVL is never released while running queries. This is the
 *   policy used when a progress handler is set. For more information see
 *   `#on_progress`.
 * - 0: The GVL is released while preparing queries, but held when iterating
 *   through records.
 * - Greater than 0: the GVL is released while preparing queries, and released
 *   periodically while iterating through records, according to the given
 *   period. A value of 1 will release the GVL on every iterated record. A value
 *   of 100 will release the GVL once for every 100 records.
 *
 * A value of nil sets the threshold to the default value, which is
 * currently 1000.
 *
 * @param threshold [Integer, nil] GVL release threshold
 * @return [Integer] GVL release threshold
 */
VALUE Database_gvl_release_threshold_set(VALUE self, VALUE value) {
  Database_t *db = self_to_open_database(self);

  switch (TYPE(value)) {
    case T_FIXNUM:
      {
        int value_int = NUM2INT(value);
        if (value_int < -1)
          rb_raise(eArgumentError, "Invalid GVL release threshold value (expect integer >= -1)");

        if (value_int > -1 && db->progress_handler.mode != PROGRESS_NONE)
          Database_reset_progress_handler(self, db);
        db->gvl_release_threshold = value_int;
        break;
      }
    case T_NIL:
      db->gvl_release_threshold = DEFAULT_GVL_RELEASE_THRESHOLD;
      break;
    default:
      rb_raise(eArgumentError, "Invalid GVL release threshold value (expect integer or nil)");
  }

  return INT2NUM(db->gvl_release_threshold);
}

void Init_ExtraliteDatabase(void) {
  VALUE mExtralite = rb_define_module("Extralite");
  rb_define_singleton_method(mExtralite, "runtime_status", Extralite_runtime_status, -1);
  rb_define_singleton_method(mExtralite, "sqlite3_version", Extralite_sqlite3_version, 0);
  rb_define_singleton_method(mExtralite, "on_progress", Extralite_on_progress, -1);

  cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
  rb_define_alloc_func(cDatabase, Database_allocate);

  rb_define_method(cDatabase, "backup",                 Database_backup, -1);
  rb_define_method(cDatabase, "batch_execute",          Database_batch_execute, 2);
  rb_define_method(cDatabase, "batch_query",            Database_batch_query, 2);
  rb_define_method(cDatabase, "batch_query_ary",        Database_batch_query_ary, 2);
  rb_define_method(cDatabase, "batch_query_splat",       Database_batch_query_splat, 2);
  rb_define_method(cDatabase, "batch_query_hash",       Database_batch_query, 2);
  rb_define_method(cDatabase, "busy_timeout=",          Database_busy_timeout_set, 1);
  rb_define_method(cDatabase, "changes",                Database_changes, 0);
  rb_define_method(cDatabase, "close",                  Database_close, 0);
  rb_define_method(cDatabase, "closed?",                Database_closed_p, 0);
  rb_define_method(cDatabase, "columns",                Database_columns, 1);
  rb_define_method(cDatabase, "errcode",                Database_errcode, 0);
  rb_define_method(cDatabase, "errmsg",                 Database_errmsg, 0);

  #ifdef HAVE_SQLITE3_ERROR_OFFSET
  rb_define_method(cDatabase, "error_offset",           Database_error_offset, 0);
  #endif

  rb_define_method(cDatabase, "execute",                Database_execute, -1);
  rb_define_method(cDatabase, "filename",               Database_filename, -1);
  rb_define_method(cDatabase, "gvl_release_threshold",  Database_gvl_release_threshold_get, 0);
  rb_define_method(cDatabase, "gvl_release_threshold=", Database_gvl_release_threshold_set, 1);
  rb_define_method(cDatabase, "initialize",             Database_initialize, -1);
  rb_define_method(cDatabase, "inspect",                Database_inspect, 0);
  rb_define_method(cDatabase, "interrupt",              Database_interrupt, 0);
  rb_define_method(cDatabase, "last_insert_rowid",      Database_last_insert_rowid, 0);
  rb_define_method(cDatabase, "limit",                  Database_limit, -1);

  #ifdef HAVE_SQLITE3_LOAD_EXTENSION
  rb_define_method(cDatabase, "load_extension",         Database_load_extension, 1);
  #endif

  rb_define_method(cDatabase, "on_progress",            Database_on_progress, -1);
  rb_define_method(cDatabase, "prepare",                Database_prepare_hash, -1);
  rb_define_method(cDatabase, "prepare_splat",           Database_prepare_splat, -1);
  rb_define_method(cDatabase, "prepare_ary",            Database_prepare_ary, -1);
  rb_define_method(cDatabase, "prepare_hash",           Database_prepare_hash, -1);
  rb_define_method(cDatabase, "query",                  Database_query, -1);
  rb_define_method(cDatabase, "query_splat",             Database_query_splat, -1);
  rb_define_method(cDatabase, "query_ary",              Database_query_ary, -1);
  rb_define_method(cDatabase, "query_hash",             Database_query, -1);
  rb_define_method(cDatabase, "query_single",           Database_query_single, -1);
  rb_define_method(cDatabase, "query_single_ary",       Database_query_single_ary, -1);
  rb_define_method(cDatabase, "query_single_splat",      Database_query_single_splat, -1);
  rb_define_method(cDatabase, "query_single_hash",      Database_query_single, -1);
  rb_define_method(cDatabase, "read_only?",             Database_read_only_p, 0);
  rb_define_method(cDatabase, "status",                 Database_status, -1);
  rb_define_method(cDatabase, "total_changes",          Database_total_changes, 0);
  rb_define_method(cDatabase, "trace",                  Database_trace, 0);

  #ifdef EXTRALITE_ENABLE_CHANGESET
  rb_define_method(cDatabase, "track_changes",          Database_track_changes, -1);
  #endif
  
  rb_define_method(cDatabase, "transaction_active?",    Database_transaction_active_p, 0);

  cBlob           = rb_define_class_under(mExtralite, "Blob", rb_cString);
  cError          = rb_define_class_under(mExtralite, "Error", rb_eStandardError);
  cSQLError       = rb_define_class_under(mExtralite, "SQLError", cError);
  cBusyError      = rb_define_class_under(mExtralite, "BusyError", cError);
  cInterruptError = rb_define_class_under(mExtralite, "InterruptError", cError);
  cParameterError = rb_define_class_under(mExtralite, "ParameterError", cError);
  eArgumentError  = rb_const_get(rb_cObject, rb_intern("ArgumentError"));

  ID_bind         = rb_intern("bind");
  ID_call         = rb_intern("call");
  ID_each         = rb_intern("each");
  ID_keys         = rb_intern("keys");
  ID_new          = rb_intern("new");
  ID_pragma       = rb_intern("pragma");
  ID_strip        = rb_intern("strip");
  ID_to_s         = rb_intern("to_s");
  ID_track        = rb_intern("track");

  SYM_at_least_once         = ID2SYM(rb_intern("at_least_once"));
  SYM_gvl_release_threshold = ID2SYM(rb_intern("gvl_release_threshold"));
  SYM_once                  = ID2SYM(rb_intern("once"));
  SYM_none                  = ID2SYM(rb_intern("none"));
  SYM_normal                = ID2SYM(rb_intern("normal"));
  SYM_pragma                = ID2SYM(rb_intern("pragma"));
  SYM_read_only             = ID2SYM(rb_intern("read_only"));
  SYM_wal                   = ID2SYM(rb_intern("wal"));

  rb_gc_register_mark_object(SYM_at_least_once);
  rb_gc_register_mark_object(SYM_gvl_release_threshold);
  rb_gc_register_mark_object(SYM_once);
  rb_gc_register_mark_object(SYM_none);
  rb_gc_register_mark_object(SYM_normal);
  rb_gc_register_mark_object(SYM_pragma);
  rb_gc_register_mark_object(SYM_read_only);
  rb_gc_register_mark_object(SYM_wal);

  rb_gc_register_mark_object(global_progress_handler.proc);

  UTF8_ENCODING = rb_utf8_encoding();
}
