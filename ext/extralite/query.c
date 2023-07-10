#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::Query
 *
 * This class represents a prepared query that can be reused with different
 * parameters. It encapsulates [SQLite prepared
 * statements](https://sqlite.org/c3ref/stmt.html).
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

static inline Query_t *self_to_query(VALUE obj) {
  Query_t *query;
  TypedData_Get_Struct((obj), Query_t, &Query_type, (query));
  return query;
}

/* Initializes a new prepared query with the given database and SQL string. A
 * `Query` is normally instantiated by calling `Database#prepare`:
 *
 *     query = @db.prepare('select * from foo')
 *
 * @param db [Extralite::Database] associated database
 * @param sql [String] SQL string
 * @return [void]
 */
VALUE Query_initialize(VALUE self, VALUE db, VALUE sql) {
  Query_t *query = self_to_query(self);

  sql = rb_funcall(sql, ID_strip, 0);
  if (!RSTRING_LEN(sql))
    rb_raise(cError, "Cannot prepare an empty SQL query");

  query->db = db;
  query->db_struct = self_to_database(db);
  query->sqlite3_db = Database_sqlite3_db(db);
  query->sql = sql;
  query->stmt = NULL;
  query->closed = 0;
  query->eof = 0;

  return Qnil;
}

static inline void query_reset(Query_t *query) {
  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);
  if (query->db_struct->trace_block != Qnil)
    rb_funcall(query->db_struct->trace_block, ID_call, 1, query->sql);
  sqlite3_reset(query->stmt);
  query->eof = 0;
}

static inline void query_reset_and_bind(Query_t *query, int argc, VALUE * argv) {
  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);

  if (query->db_struct->trace_block != Qnil)
    rb_funcall(query->db_struct->trace_block, ID_call, 1, query->sql);

  sqlite3_reset(query->stmt);
  query->eof = 0;
  if (argc > 0) {
    sqlite3_clear_bindings(query->stmt);
    bind_all_parameters(query->stmt, argc, argv);
  }
}

/* Resets the underlying prepared statement. After calling this method the
 * underlying prepared statement is reset to its initial state, and any call to
 * one of the `#next_xxx` methods will return the first row in the query's
 * result set.
 *
 *     query = @db.prepare('select * from foo where bar = ?')
 *     first = query.next
 *     second = query.next
 *     query.reset
 *     query.next #=> returns the first row again
 *
 * @return [Extralite::Query] self
 */
VALUE Query_reset(VALUE self) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  query_reset(query);
  if (query->db_struct->trace_block != Qnil)
    rb_funcall(query->db_struct->trace_block, ID_call, 1, query->sql);

  return self;
}

/* Resets the underlying prepared statement and rebinds parameters if any are
 * given. After calling this method the underlying prepared statement is reset
 * to its initial state, and any call to one of the `#next_xxx` methods will
 * return the first row in the query's result set.
 *
 * Bound parameters can be specified as a list of values or as a hash mapping
 * parameter names to values. When parameters are given as a splatted array, the
 * query should specify parameters using `?`:
 * 
 *     query = db.prepare('select * from foo where x = ?')
 *     query.bind(42)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 * 
 *     query = db.prepare('select * from foo where x = :bar')
 *     query.bind(bar: 42)
 *
 * @return [Extralite::Query] self
 */
VALUE Query_bind(int argc, VALUE *argv, VALUE self) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  query_reset_and_bind(query, argc, argv);
  return self;
}

/* Returns true if iteration has reached the end of the result set.
 *
 * @return [boolean] true if iteration has reached the end of the result set
 */
VALUE Query_eof_p(VALUE self) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  return query->eof ? Qtrue : Qfalse;
}

#define MAX_ROWS(max_rows) (max_rows == SINGLE_ROW ? 1 : max_rows)

static inline VALUE Query_perform_next(VALUE self, int max_rows, VALUE (*call)(query_ctx *)) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");
  
  if (!query->stmt) query_reset(query);
  if (query->eof) return rb_block_given_p() ? self : Qnil;

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

/* Returns the next 1 or more rows from the associated query's result set as a
 * hash.
 * 
 * If no row count is given, a single row is returned. If a row count is given,
 * an array containing up to the `row_count` rows is returned. If `row_count` is
 * -1, all rows are returned. If the end of the result set has been reached,
 * `nil` is returned.
 * 
 * If a block is given, rows are passed to the block and self is returned.
 *
 * @overload next()
 *   @return [Hash, Extralite::Query] next row or self if block is given
 * @overload next_hash()
 *   @return [Hash, Extralite::Query] next row or self if block is given
 * @overload next(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array<Hash>, Extralite::Query] next rows or self if block is given
 * @overload next_hash(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array<Hash>, Extralite::Query] next rows or self if block is given
 */
VALUE Query_next_hash(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_hash);
}

/* Returns the next 1 or more rows from the associated query's result set as an
 * array.
 * 
 * If no row count is given, a single row is returned. If a row count is given,
 * an array containing up to the `row_count` rows is returned. If `row_count` is
 * -1, all rows are returned. If the end of the result set has been reached,
 * `nil` is returned.
 * 
 * If a block is given, rows are passed to the block and self is returned.
 *
 * @overload next_ary()
 *   @return [Array, Extralite::Query] next row or self if block is given
 * @overload next_ary(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array<Array>, Extralite::Query] next rows or self if block is given
 */
VALUE Query_next_ary(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_ary);
}

/* Returns the next 1 or more rows from the associated query's result set as an
 * single values. If the result set contains more than one column an error is
 * raised.
 * 
 * If no row count is given, a single row is returned. If a row count is given,
 * an array containing up to the `row_count` rows is returned. If `row_count` is
 * -1, all rows are returned. If the end of the result set has been reached,
 * `nil` is returned.
 * 
 * If a block is given, rows are passed to the block and self is returned.
 *
 * @overload next_ary()
 *   @return [Object, Extralite::Query] next row or self if block is given
 * @overload next_ary(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array<Object>, Extralite::Query] next rows or self if block is given
 */
VALUE Query_next_single_column(int argc, VALUE *argv, VALUE self) {
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), safe_query_single_column);
}

/* Returns all rows in the associated query's result set as hashes.
 * 
 * @overload to_a()
 *   @return [Array<Hash>] all rows
 * @overload to_a_hash
 *   @return [Array<Hash>] all rows
 */
VALUE Query_to_a_hash(VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_hash);
}

/* Returns all rows in the associated query's result set as arrays.
 * 
 * @return [Array<Array>] all rows
 */
VALUE Query_to_a_ary(VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_ary);
}

/* Returns all rows in the associated query's result set as single values. If
 * the result set contains more than one column an error is raised.
 * 
 * @return [Array<Object>] all rows
 */
VALUE Query_to_a_single_column(VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_single_column);
}

/* Iterates through the result set, passing each row to the given block as a
 * hash. If no block is given, returns a `Extralite::Iterator` instance in hash
 * mode.
 * 
 * @return [Extralite::Query, Extralite::Iterator] self or an iterator if no block is given
 */
VALUE Query_each_hash(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_hash);

  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_hash);
}

/* Iterates through the result set, passing each row to the given block as an
 * array. If no block is given, returns a `Extralite::Iterator` instance in
 * array mode.
 * 
 * @return [Extralite::Query, Extralite::Iterator] self or an iterator if no block is given
 */
VALUE Query_each_ary(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_ary);

  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_ary);
}

/* Iterates through the result set, passing each row to the given block as a
 * single value. If the result set contains more than one column an error is
 * raised. If no block is given, returns a `Extralite::Iterator` instance in
 * single column mode.
 * 
 * @return [Extralite::Query, Extralite::Iterator] self or an iterator if no block is given
 */
VALUE Query_each_single_column(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 2, self, SYM_single_column);

  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_single_column);
}

/* Executes the query for each set of parameters in the given array. Parameters
 * can be specified as either an array (for unnamed parameters) or a hash (for
 * named parameters). Returns the number of changes effected. This method is
 * designed for inserting multiple records.
 *
 *     query = db.prepare('insert into foo values (?, ?, ?)')
 *     records = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     query.execute_multi(records)
 *
 * @param parameters [Array<Array, Hash>] array of parameters to run query with
 * @return [Integer] number of changes effected
 */
VALUE Query_execute_multi(VALUE self, VALUE parameters) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(query->sqlite3_db, &query->stmt, query->sql);

  query_ctx ctx = { self, query->sqlite3_db, query->stmt, parameters, QUERY_MODE(QUERY_MULTI_ROW), ALL_ROWS };
  return safe_execute_multi(&ctx);
}

/* Returns the database associated with the query.
 *
 * @overload database()
 *   @return [Extralite::Database] associated database
 * @overload db()
 *   @return [Extralite::Database] associated database
 */
VALUE Query_database(VALUE self) {
  Query_t *query = self_to_query(self);
  return query->db;
}

/* Returns the SQL string for the query.
 * 
 * @return [String] SQL string
 */
VALUE Query_sql(VALUE self) {
  Query_t *query = self_to_query(self);
  return query->sql;
}

/* Returns the column names for the query without running it.
 * 
 * @return [Array<Symbol>] column names
 */
VALUE Query_columns(VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, safe_query_columns);
}

/* Closes the query. Attempting to run a closed query will raise an error.
 * 
 * @return [Extralite::Query] self
 */
VALUE Query_close(VALUE self) {
  Query_t *query = self_to_query(self);
  if (query->stmt) {
    sqlite3_finalize(query->stmt);
    query->stmt = NULL;
  }
  query->closed = 1;
  return self;
}

/* Returns true if the query is closed.
 * 
 * @return [boolean] true if query is closed
 */
VALUE Query_closed_p(VALUE self) {
  Query_t *query = self_to_query(self);
  return query->closed ? Qtrue : Qfalse;
}

/* Returns the current [status
 * value](https://sqlite.org/c3ref/c_stmtstatus_counter.html) for the given op.
 * To reset the value, pass true as reset.
 * 
 * @overload status(op)
 *   @param op [Integer] status op
 *   @return [Integer] current status value for the given op
 * @overload status(op, reset)
 *   @param op [Integer] status op
 *   @param reset [true] reset flag
 *   @return [Integer] current status value for the given op (before reset)
 */
VALUE Query_status(int argc, VALUE* argv, VALUE self) {
  VALUE op, reset;

  rb_scan_args(argc, argv, "11", &op, &reset);

  Query_t *query = self_to_query(self);
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

  rb_define_method(cQuery, "bind", Query_bind, -1);
  rb_define_method(cQuery, "close", Query_close, 0);
  rb_define_method(cQuery, "closed?", Query_closed_p, 0);
  rb_define_method(cQuery, "columns", Query_columns, 0);
  rb_define_method(cQuery, "database", Query_database, 0);
  rb_define_method(cQuery, "db", Query_database, 0);

  rb_define_method(cQuery, "each", Query_each_hash, 0);
  rb_define_method(cQuery, "each_ary", Query_each_ary, 0);
  rb_define_method(cQuery, "each_hash", Query_each_hash, 0);
  rb_define_method(cQuery, "each_single_column", Query_each_single_column, 0);

  rb_define_method(cQuery, "eof?", Query_eof_p, 0);
  rb_define_method(cQuery, "execute_multi", Query_execute_multi, 1);
  rb_define_method(cQuery, "initialize", Query_initialize, 2);

  rb_define_method(cQuery, "next", Query_next_hash, -1);
  rb_define_method(cQuery, "next_ary", Query_next_ary, -1);
  rb_define_method(cQuery, "next_hash", Query_next_hash, -1);
  rb_define_method(cQuery, "next_single_column", Query_next_single_column, -1);

  rb_define_method(cQuery, "reset", Query_reset, 0);
  rb_define_method(cQuery, "sql", Query_sql, 0);
  rb_define_method(cQuery, "status", Query_status, -1);

  rb_define_method(cQuery, "to_a", Query_to_a_hash, 0);
  rb_define_method(cQuery, "to_a_ary", Query_to_a_ary, 0);
  rb_define_method(cQuery, "to_a_hash", Query_to_a_hash, 0);
  rb_define_method(cQuery, "to_a_single_column", Query_to_a_single_column, 0);
}
