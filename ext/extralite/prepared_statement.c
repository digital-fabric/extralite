#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::PreparedStatement
 *
 * This class represents a prepared statement.
 */

VALUE cPreparedStatement;

static size_t PreparedStatement_size(const void *ptr) {
  return sizeof(PreparedStatement_t);
}

static void PreparedStatement_mark(void *ptr) {
  PreparedStatement_t *stmt = ptr;
  rb_gc_mark(stmt->db);
  rb_gc_mark(stmt->sql);
}

static void PreparedStatement_free(void *ptr) {
  PreparedStatement_t *stmt = ptr;
  if (stmt->stmt) sqlite3_finalize(stmt->stmt);
  free(ptr);
}

static const rb_data_type_t PreparedStatement_type = {
    "PreparedStatement",
    {PreparedStatement_mark, PreparedStatement_free, PreparedStatement_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE PreparedStatement_allocate(VALUE klass) {
  PreparedStatement_t *stmt = ALLOC(PreparedStatement_t);
  stmt->db = Qnil;
  stmt->sqlite3_db = NULL;
  stmt->stmt = NULL;
  return TypedData_Wrap_Struct(klass, &PreparedStatement_type, stmt);
}

#define GetPreparedStatement(obj, stmt) \
  TypedData_Get_Struct((obj), PreparedStatement_t, &PreparedStatement_type, (stmt))

/* call-seq: initialize(db, sql)
 *
 * Initializes a new SQLite prepared statement with the given path.
 */
VALUE PreparedStatement_initialize(VALUE self, VALUE db, VALUE sql) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);

  sql = rb_funcall(sql, ID_strip, 0);
  if (!RSTRING_LEN(sql))
    rb_raise(cError, "Cannot prepare an empty SQL query");

  stmt->db = db;
  stmt->db_struct = Database_struct(db);
  stmt->sqlite3_db = Database_sqlite3_db(db);
  stmt->sql = sql;

  prepare_single_stmt(stmt->sqlite3_db, &stmt->stmt, sql);

  return Qnil;
}

static inline VALUE PreparedStatement_perform_query(int argc, VALUE *argv, VALUE self, VALUE (*call)(query_ctx *)) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);

  if (!stmt->stmt)
    rb_raise(cError, "Prepared statement is closed");

  if (stmt->db_struct->trace_block != Qnil) rb_funcall(stmt->db_struct->trace_block, ID_call, 1, stmt->sql);

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
VALUE PreparedStatement_query_hash(int argc, VALUE *argv, VALUE self) {
  return PreparedStatement_perform_query(argc, argv, self, safe_query_hash);
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
VALUE PreparedStatement_query_ary(int argc, VALUE *argv, VALUE self) {
  return PreparedStatement_perform_query(argc, argv, self, safe_query_ary);
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
VALUE PreparedStatement_query_single_row(int argc, VALUE *argv, VALUE self) {
  return PreparedStatement_perform_query(argc, argv, self, safe_query_single_row);
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
VALUE PreparedStatement_query_single_column(int argc, VALUE *argv, VALUE self) {
  return PreparedStatement_perform_query(argc, argv, self, safe_query_single_column);
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
VALUE PreparedStatement_query_single_value(int argc, VALUE *argv, VALUE self) {
  return PreparedStatement_perform_query(argc, argv, self, safe_query_single_value);
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
VALUE PreparedStatement_execute_multi(VALUE self, VALUE params_array) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);

  if (!stmt->stmt)
    rb_raise(cError, "Prepared statement is closed");

  query_ctx ctx = { self, stmt->sqlite3_db, stmt->stmt, params_array };
  return safe_execute_multi(&ctx);
}

/* call-seq:
 *   stmt.database -> database
 *   stmt.db -> database
 *
 * Returns the database associated with the prepared statement.
 */
VALUE PreparedStatement_database(VALUE self) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);
  return stmt->db;
}

/* call-seq:
 *   stmt.sql -> sql
 *
 * Returns the SQL query used for the prepared statement.
 */
VALUE PreparedStatement_sql(VALUE self) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);
  return stmt->sql;
}

/* call-seq:
 *   stmt.columns -> columns
 *
 * Returns the column names for the prepared statement without running it.
 */
VALUE PreparedStatement_columns(VALUE self) {
  return PreparedStatement_perform_query(0, NULL, self, safe_query_columns);
}

/* call-seq:
 *   stmt.close -> stmt
 *
 * Closes the prepared statement. Running a closed prepared statement will raise
 * an error.
 */
VALUE PreparedStatement_close(VALUE self) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);
  if (stmt->stmt) {
    sqlite3_finalize(stmt->stmt);
    stmt->stmt = NULL;
  }
  return self;
}

/* call-seq:
 *   stmt.closed? -> closed
 *
 * Returns true if the prepared statement is closed.
 */
VALUE PreparedStatement_closed_p(VALUE self) {
  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);

  return stmt->stmt ? Qfalse : Qtrue;
}

/* call-seq:
 *   stmt.status(op[, reset]) -> value
 *
 * Returns the current status value for the given op. To reset the value, pass
 * true as reset.
 */
VALUE PreparedStatement_status(int argc, VALUE* argv, VALUE self) {
  VALUE op, reset;

  rb_scan_args(argc, argv, "11", &op, &reset);

  PreparedStatement_t *stmt;
  GetPreparedStatement(self, stmt);

  int value = sqlite3_stmt_status(stmt->stmt, NUM2INT(op), RTEST(reset) ? 1 : 0);
  return INT2NUM(value);
}

void Init_ExtralitePreparedStatement(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cPreparedStatement = rb_define_class_under(mExtralite, "PreparedStatement", rb_cObject);
  rb_define_alloc_func(cPreparedStatement, PreparedStatement_allocate);

  rb_define_method(cPreparedStatement, "close", PreparedStatement_close, 0);
  rb_define_method(cPreparedStatement, "closed?", PreparedStatement_closed_p, 0);
  rb_define_method(cPreparedStatement, "columns", PreparedStatement_columns, 0);
  rb_define_method(cPreparedStatement, "database", PreparedStatement_database, 0);
  rb_define_method(cPreparedStatement, "db", PreparedStatement_database, 0);
  rb_define_method(cPreparedStatement, "execute_multi", PreparedStatement_execute_multi, 1);
  rb_define_method(cPreparedStatement, "initialize", PreparedStatement_initialize, 2);
  rb_define_method(cPreparedStatement, "query", PreparedStatement_query_hash, -1);
  rb_define_method(cPreparedStatement, "query_hash", PreparedStatement_query_hash, -1);
  rb_define_method(cPreparedStatement, "query_ary", PreparedStatement_query_ary, -1);
  rb_define_method(cPreparedStatement, "query_single_row", PreparedStatement_query_single_row, -1);
  rb_define_method(cPreparedStatement, "query_single_column", PreparedStatement_query_single_column, -1);
  rb_define_method(cPreparedStatement, "query_single_value", PreparedStatement_query_single_value, -1);
  rb_define_method(cPreparedStatement, "sql", PreparedStatement_sql, 0);
  rb_define_method(cPreparedStatement, "status", PreparedStatement_status, -1);
}
