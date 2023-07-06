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
  Query_t *query = ptr;
  rb_gc_mark(query->db);
  rb_gc_mark(query->sql);
}

static void Query_free(void *ptr) {
  Query_t *query = ptr;
  if (query->stmt) sqlite3_finalize(query->stmt);
  free(ptr);
}

static const rb_data_type_t Query_type = {
    "Query",
    {Query_mark, Query_free, Query_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Query_allocate(VALUE klass) {
  Query_t *query = ALLOC(Query_t);
  query->db = Qnil;
  query->sql = Qnil;
  query->sqlite3_db = NULL;
  query->stmt = NULL;
  return TypedData_Wrap_Struct(klass, &Query_type, query);
}

static inline Query_t *value_to_query(VALUE obj) {
  Query_t *query;
  TypedData_Get_Struct((obj), Query_t, &Query_type, (query));
  return query;
}

/* call-seq: initialize(db, sql)
 *
 * Initializes a new prepared query with the given database and SQL string.
 */
VALUE Query_initialize(VALUE self, VALUE db, VALUE sql) {
  Query_t *query = value_to_query(self);

  sql = rb_funcall(sql, ID_strip, 0);
  if (!RSTRING_LEN(sql))
    rb_raise(cError, "Cannot prepare an empty SQL query");

  query->db = db;
  query->db_struct = Database_struct(db);
  query->sqlite3_db = Database_sqlite3_db(db);
  query->sql = sql;
  query->stmt = NULL;
  query->closed = 0;
  query->eof = 0;

  return Qnil;
}

void query_reset_and_bind(Query_t *query, int argc, VALUE * argv) {
  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);
  sqlite3_reset(query->stmt);
  query->eof = 0;
  if (argc > 0) {
    sqlite3_clear_bindings(query->stmt);
    bind_all_parameters(query->stmt, argc, argv);
  }
}

VALUE Query_reset(int argc, VALUE *argv, VALUE self) {
  Query_t *query = value_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  query_reset_and_bind(query, argc, argv);
  return self;
}

#define MAX_ROWS(max_rows) (max_rows == SINGLE_ROW ? 1 : max_rows)

static inline VALUE Query_perform_next(VALUE self, int max_rows, VALUE (*call)(query_ctx *)) {
  Query_t *query = value_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");
  
  if (!query->stmt) query_reset_and_bind(query, 0, NULL);
  if (query->eof) return rb_block_given_p() ? self : Qnil;

  if (query->db_struct->trace_block != Qnil)
    rb_funcall(query->db_struct->trace_block, ID_call, 1, query->sql);

  query_ctx ctx = {
    self,
    query->sqlite3_db,
    query->stmt,
    Qnil,
    QUERY_MODE(max_rows == SINGLE_ROW ? QUERY_SINGLE_ROW : QUERY_MULTI_ROW),
    MAX_ROWS(max_rows),
    0
  };
  VALUE result = call(&ctx);
  query->eof = ctx.eof;
  return (ctx.mode == QUERY_YIELD) ? self : result;
}

#define MAX_ROWS_FROM_ARGV(argc, argv) (argc == 1 ? FIX2INT(argv[0]) : SINGLE_ROW)

VALUE Query_next_hash(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_hash);
}

VALUE Query_next_ary(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_ary);
}

VALUE Query_next_single_column(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_single_column);
}

VALUE Query_to_a_hash(VALUE self) {
  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_hash);
}

VALUE Query_to_a_ary(VALUE self) {
  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_ary);
}

VALUE Query_to_a_single_column(VALUE self) {
  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_single_column);
}

VALUE Query_each_hash(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_hash);

  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_hash);
}

VALUE Query_each_ary(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_ary);

  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_ary);
}

VALUE Query_each_single_column(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_single_column);

  Query_t *query = value_to_query(self);
  query_reset_and_bind(query, 0, NULL);
  return Query_perform_next(self, ALL_ROWS, safe_query_single_column);
}

static inline VALUE Query_perform_query(int argc, VALUE *argv, VALUE self, VALUE (*call)(query_ctx *)) {
  Query_t *query = value_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);
  if (query->db_struct->trace_block != Qnil)
    rb_funcall(query->db_struct->trace_block, ID_call, 1, query->sql);

  sqlite3_reset(query->stmt);
  sqlite3_clear_bindings(query->stmt);
  bind_all_parameters(query->stmt, argc, argv);
  query_ctx ctx = { self, query->sqlite3_db, query->stmt, Qnil, QUERY_MODE(QUERY_MULTI_ROW), ALL_ROWS };
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
  Query_t *query = value_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);

  query_ctx ctx = { self, query->sqlite3_db, query->stmt, params_array, QUERY_MODE(QUERY_MULTI_ROW), ALL_ROWS };
  return safe_execute_multi(&ctx);
}

/* call-seq:
 *   stmt.database -> database
 *   stmt.db -> database
 *
 * Returns the database associated with the query.
 */
VALUE Query_database(VALUE self) {
  Query_t *query = value_to_query(self);
  return query->db;
}

/* call-seq:
 *   stmt.sql -> sql
 *
 * Returns the SQL string for the query.
 */
VALUE Query_sql(VALUE self) {
  Query_t *query = value_to_query(self);
  return query->sql;
}

/* call-seq:
 *   stmt.columns -> columns
 *
 * Returns the column names for the query without running it.
 */
VALUE Query_columns(VALUE self) {
  return Query_perform_query(0, NULL, self, safe_query_columns);
}

/* call-seq:
 *   stmt.close -> stmt
 *
 * Closes the query. Attempting to run a closed prepared statement will raise an
 * error.
 */
VALUE Query_close(VALUE self) {
  Query_t *query = value_to_query(self);
  if (query->stmt) {
    sqlite3_finalize(query->stmt);
    query->stmt = NULL;
  }
  query->closed = 1;
  return self;
}

/* call-seq:
 *   stmt.closed? -> closed
 *
 * Returns true if the query is closed.
 */
VALUE Query_closed_p(VALUE self) {
  Query_t *query = value_to_query(self);
  return query->closed ? Qtrue : Qfalse;
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

  Query_t *query = value_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);

  int value = sqlite3_stmt_status(query->stmt, NUM2INT(op), RTEST(reset) ? 1 : 0);
  return INT2NUM(value);
}

void Init_ExtraliteQuery(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cQuery = rb_define_class_under(mExtralite, "Query", rb_cObject);
  rb_define_alloc_func(cQuery, Query_allocate);

  rb_define_method(cQuery, "bind", Query_reset, -1);
  rb_define_method(cQuery, "close", Query_close, 0);
  rb_define_method(cQuery, "closed?", Query_closed_p, 0);
  rb_define_method(cQuery, "columns", Query_columns, 0);
  rb_define_method(cQuery, "database", Query_database, 0);
  rb_define_method(cQuery, "db", Query_database, 0);
  rb_define_method(cQuery, "execute_multi", Query_execute_multi, 1);
  rb_define_method(cQuery, "initialize", Query_initialize, 2);

  rb_define_method(cQuery, "each", Query_each_hash, 0);
  rb_define_method(cQuery, "each_ary", Query_each_ary, 0);
  rb_define_method(cQuery, "each_hash", Query_each_hash, 0);
  rb_define_method(cQuery, "each_single_column", Query_each_single_column, 0);

  rb_define_method(cQuery, "next", Query_next_hash, -1);
  rb_define_method(cQuery, "next_ary", Query_next_ary, -1);
  rb_define_method(cQuery, "next_hash", Query_next_hash, -1);
  rb_define_method(cQuery, "next_single_column", Query_next_single_column, -1);

  rb_define_method(cQuery, "to_a", Query_to_a_hash, 0);
  rb_define_method(cQuery, "to_a_ary", Query_to_a_ary, 0);
  rb_define_method(cQuery, "to_a_hash", Query_to_a_hash, 0);
  rb_define_method(cQuery, "to_a_single_column", Query_to_a_single_column, 0);

  rb_define_method(cQuery, "reset", Query_reset, -1);
  rb_define_method(cQuery, "sql", Query_sql, 0);
  rb_define_method(cQuery, "status", Query_status, -1);
}
