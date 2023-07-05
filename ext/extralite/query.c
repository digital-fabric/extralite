#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::Query
 *
 * This class represents a prepared statement.
 */

VALUE cQuery;

static size_t Query_size(const void *ptr) {
  return sizeof(Query_t);
}

static void Query_mark(void *ptr) {
  Query_t *stmt = ptr;
  rb_gc_mark(stmt->db);
  rb_gc_mark(stmt->sql);
}

static void Query_free(void *ptr) {
  Query_t *stmt = ptr;
  if (stmt->stmt) sqlite3_finalize(stmt->stmt);
  free(ptr);
}

static const rb_data_type_t Query_type = {
    "Query",
    {Query_mark, Query_free, Query_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Query_allocate(VALUE klass) {
  Query_t *stmt = ALLOC(Query_t);
  stmt->db = Qnil;
  stmt->sqlite3_db = NULL;
  stmt->stmt = NULL;
  return TypedData_Wrap_Struct(klass, &Query_type, stmt);
}

#define GetQuery(obj, stmt) \
  TypedData_Get_Struct((obj), Query_t, &Query_type, (stmt))

/* call-seq: initialize(db, sql)
 *
 * Initializes a new SQLite prepared statement with the given path.
 */
VALUE Query_initialize(VALUE self, VALUE db, VALUE sql) {
  Query_t *stmt;
  GetQuery(self, stmt);

  sql = rb_funcall(sql, ID_strip, 0);
  if (!RSTRING_LEN(sql))
    rb_raise(cError, "Cannot prepare an empty SQL query");

  stmt->db = db;
  stmt->db_struct = Database_struct(db);
  stmt->sqlite3_db = Database_sqlite3_db(db);
  stmt->sql = sql;
  stmt->stmt = 0L;
  stmt->closed = 0;

  return Qnil;
}

static inline VALUE Query_perform_query(int argc, VALUE *argv, VALUE self, VALUE (*call)(query_ctx *)) {
  Query_t *stmt;
  GetQuery(self, stmt);

  if (stmt->closed)
    rb_raise(cError, "Prepared statement is closed");

  if (!stmt->stmt)
    prepare_single_stmt(stmt->sqlite3_db, &stmt->stmt, stmt->sql);
  if (stmt->db_struct->trace_block != Qnil)
    rb_funcall(stmt->db_struct->trace_block, ID_call, 1, stmt->sql);

  sqlite3_reset(stmt->stmt);
  sqlite3_clear_bindings(stmt->stmt);
  bind_all_parameters(stmt->stmt, argc, argv);
  query_ctx ctx = { self, stmt->sqlite3_db, stmt->stmt };
  return call(&ctx);
}

/* call-seq:
 *    query(sql, *parameters, &block) -> [...]
 *    query_hash(sql, *parameters, &block) -> [...]
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
VALUE Query_query_hash(int argc, VALUE *argv, VALUE self) {
  return Query_perform_query(argc, argv, self, safe_query_hash);
}

/* call-seq:
 *   stmt.query_ary(sql, *parameters, &block) -> [...]
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
VALUE Query_query_ary(int argc, VALUE *argv, VALUE self) {
  return Query_perform_query(argc, argv, self, safe_query_ary);
}

/* call-seq:
 *   stmt.query_single_row(sql, *parameters) -> {...}
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
VALUE Query_query_single_row(int argc, VALUE *argv, VALUE self) {
  return Query_perform_query(argc, argv, self, safe_query_single_row);
}

/* call-seq:
 *   stmt.query_single_column(sql, *parameters, &block) -> [...]
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
VALUE Query_query_single_column(int argc, VALUE *argv, VALUE self) {
  return Query_perform_query(argc, argv, self, safe_query_single_column);
}

/* call-seq:
 *   stmt.query_single_value(sql, *parameters) -> value
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
VALUE Query_query_single_value(int argc, VALUE *argv, VALUE self) {
  return Query_perform_query(argc, argv, self, safe_query_single_value);
}

/* call-seq:
 *   stmt.execute_multi(params_array) -> changes
 *
 * Executes the prepared statment for each list of parameters in params_array.
 * Returns the number of changes effected. This method is designed for inserting
 * multiple records.
 *
 *     stmt = db.prepare('insert into foo values (?, ?, ?)')
 *     records = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     stmt.execute_multi_query(records)
 *
 */
VALUE Query_execute_multi(VALUE self, VALUE params_array) {
  Query_t *stmt;
  GetQuery(self, stmt);

  if (stmt->closed)
    rb_raise(cError, "Prepared statement is closed");

  if (!stmt->stmt)
    prepare_single_stmt(stmt->sqlite3_db, &stmt->stmt, stmt->sql);

  query_ctx ctx = { self, stmt->sqlite3_db, stmt->stmt, params_array };
  return safe_execute_multi(&ctx);
}

/* call-seq:
 *   stmt.database -> database
 *   stmt.db -> database
 *
 * Returns the database associated with the prepared statement.
 */
VALUE Query_database(VALUE self) {
  Query_t *stmt;
  GetQuery(self, stmt);
  return stmt->db;
}

/* call-seq:
 *   stmt.sql -> sql
 *
 * Returns the SQL query used for the prepared statement.
 */
VALUE Query_sql(VALUE self) {
  Query_t *stmt;
  GetQuery(self, stmt);
  return stmt->sql;
}

/* call-seq:
 *   stmt.columns -> columns
 *
 * Returns the column names for the prepared statement without running it.
 */
VALUE Query_columns(VALUE self) {
  return Query_perform_query(0, NULL, self, safe_query_columns);
}

/* call-seq:
 *   stmt.close -> stmt
 *
 * Closes the prepared statement. Running a closed prepared statement will raise
 * an error.
 */
VALUE Query_close(VALUE self) {
  Query_t *stmt;
  GetQuery(self, stmt);
  if (stmt->stmt) {
    sqlite3_finalize(stmt->stmt);
    stmt->stmt = NULL;
  }
  stmt->closed = 1;
  return self;
}

/* call-seq:
 *   stmt.closed? -> closed
 *
 * Returns true if the prepared statement is closed.
 */
VALUE Query_closed_p(VALUE self) {
  Query_t *stmt;
  GetQuery(self, stmt);

  return stmt->closed ? Qtrue : Qfalse;
}

/* call-seq:
 *   stmt.status(op[, reset]) -> value
 *
 * Returns the current status value for the given op. To reset the value, pass
 * true as reset.
 */
VALUE Query_status(int argc, VALUE* argv, VALUE self) {
  VALUE op, reset;

  rb_scan_args(argc, argv, "11", &op, &reset);

  Query_t *stmt;
  GetQuery(self, stmt);

  if (stmt->closed)
    rb_raise(cError, "Prepared statement is closed");

  if (!stmt->stmt)
    prepare_single_stmt(stmt->sqlite3_db, &stmt->stmt, stmt->sql);

  int value = sqlite3_stmt_status(stmt->stmt, NUM2INT(op), RTEST(reset) ? 1 : 0);
  return INT2NUM(value);
}

void Init_ExtraliteQuery(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cQuery = rb_define_class_under(mExtralite, "Query", rb_cObject);
  rb_define_alloc_func(cQuery, Query_allocate);

  rb_define_method(cQuery, "close", Query_close, 0);
  rb_define_method(cQuery, "closed?", Query_closed_p, 0);
  rb_define_method(cQuery, "columns", Query_columns, 0);
  rb_define_method(cQuery, "database", Query_database, 0);
  rb_define_method(cQuery, "db", Query_database, 0);
  rb_define_method(cQuery, "execute_multi", Query_execute_multi, 1);
  rb_define_method(cQuery, "initialize", Query_initialize, 2);
  rb_define_method(cQuery, "query", Query_query_hash, -1);
  rb_define_method(cQuery, "query_hash", Query_query_hash, -1);
  rb_define_method(cQuery, "query_ary", Query_query_ary, -1);
  rb_define_method(cQuery, "query_single_row", Query_query_single_row, -1);
  rb_define_method(cQuery, "query_single_column", Query_query_single_column, -1);
  rb_define_method(cQuery, "query_single_value", Query_query_single_value, -1);
  rb_define_method(cQuery, "sql", Query_sql, 0);
  rb_define_method(cQuery, "status", Query_status, -1);
}
