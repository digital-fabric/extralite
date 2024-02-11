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

ID ID_inspect;
ID ID_slice;

VALUE SYM_hash;
VALUE SYM_argv;
VALUE SYM_ary;

#define DB_GVL_MODE(query) Database_prepare_gvl_mode(query->db_struct)

static size_t Query_size(const void *ptr) {
  return sizeof(Query_t);
}

static void Query_mark(void *ptr) {
  Query_t *query = ptr;
  rb_gc_mark_movable(query->db);
  rb_gc_mark_movable(query->sql);
  rb_gc_mark_movable(query->transform_proc);
}

static void Query_compact(void *ptr) {
  Query_t *query = ptr;
  query->db = rb_gc_location(query->db);
  query->sql = rb_gc_location(query->sql);
  query->transform_proc = rb_gc_location(query->transform_proc);
}

static void Query_free(void *ptr) {
  Query_t *query = ptr;
  if (query->stmt) sqlite3_finalize(query->stmt);
  free(ptr);
}

static const rb_data_type_t Query_type = {
    "Query",
    {Query_mark, Query_free, Query_size, Query_compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static VALUE Query_allocate(VALUE klass) {
  Query_t *query = ALLOC(Query_t);
  query->db = Qnil;
  query->sql = Qnil;
  query->transform_proc = Qnil;
  query->sqlite3_db = NULL;
  query->stmt = NULL;
  return TypedData_Wrap_Struct(klass, &Query_type, query);
}

static inline Query_t *self_to_query(VALUE obj) {
  Query_t *query;
  TypedData_Get_Struct((obj), Query_t, &Query_type, (query));
  return query;
}

static inline enum query_mode symbol_to_query_mode(VALUE sym) {
  if (sym == SYM_hash)          return QUERY_HASH;
  if (sym == SYM_argv)          return QUERY_ARGV;
  if (sym == SYM_ary)           return QUERY_ARY;

  rb_raise(cError, "Invalid query mode");
}

static inline VALUE query_mode_to_symbol(enum query_mode query_mode) {
  switch (query_mode) {
    case QUERY_HASH:
      return SYM_hash;
    case QUERY_ARGV:
      return SYM_argv;
    case QUERY_ARY:
      return SYM_ary;
    default:
      rb_raise(cError, "Invalid mode");
  }
}

/* Initializes a new prepared query with the given database and SQL string. A
 * `Query` is normally instantiated by calling `Database#prepare`:
 *
 *     query = @db.prepare('select * from foo')
 *
 * @param db [Extralite::Database] associated database
 * @param sql [String] SQL string
 * @param mode [Symbol] query mode
 * @return [void]
 */
VALUE Query_initialize(VALUE self, VALUE db, VALUE sql, VALUE mode) {
  Query_t *query = self_to_query(self);

  sql = rb_funcall(sql, ID_strip, 0);
  if (!RSTRING_LEN(sql))
    rb_raise(cError, "Cannot prepare an empty SQL query");

  RB_OBJ_WRITE(self, &query->db, db);
  RB_OBJ_WRITE(self, &query->sql, sql);
  if (rb_block_given_p())
    RB_OBJ_WRITE(self, &query->transform_proc, rb_block_proc());

  query->db = db;
  query->db_struct = self_to_database(db);
  query->sqlite3_db = Database_sqlite3_db(db);
  query->stmt = NULL;
  query->closed = 0;
  query->eof = 0;
  query->query_mode = symbol_to_query_mode(mode);

  return Qnil;
}

static inline void query_reset(Query_t *query) {
  if (!query->stmt)
    prepare_single_stmt(DB_GVL_MODE(query), query->sqlite3_db, &query->stmt, query->sql);
  Database_issue_query(query->db_struct, query->sql);
  sqlite3_reset(query->stmt);
  query->eof = 0;
}

static inline void query_reset_and_bind(Query_t *query, int argc, VALUE * argv) {
  if (!query->stmt)
    prepare_single_stmt(DB_GVL_MODE(query), query->sqlite3_db, &query->stmt, query->sql);
  Database_issue_query(query->db_struct, query->sql);
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

static inline VALUE Query_perform_next(VALUE self, int max_rows, safe_query_impl call) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt) query_reset(query);
  if (query->eof) return rb_block_given_p() ? self : Qnil;

  query_ctx ctx = QUERY_CTX(
    self,
    query->sql,
    query->db_struct,
    query->stmt,
    Qnil,
    query->transform_proc,
    query->query_mode,
    ROW_YIELD_OR_MODE(max_rows == SINGLE_ROW ? ROW_SINGLE : ROW_MULTI),
    MAX_ROWS(max_rows)
  );
  VALUE result = call(&ctx);
  query->eof = ctx.eof;
  return (ctx.row_mode == ROW_YIELD) ? self : result;
}

#define MAX_ROWS_FROM_ARGV(argc, argv) (argc == 1 ? FIX2INT(argv[0]) : SINGLE_ROW)

inline safe_query_impl query_impl(enum query_mode query_mode) {
  switch (query_mode) {
    case QUERY_HASH:
      return safe_query_hash;
    case QUERY_ARGV:
      return safe_query_argv;
    case QUERY_ARY:
      return safe_query_ary;
    default:
      rb_raise(cError, "Invalid query mode (query_impl)");
  }
}

/* Returns the next 1 or more rows from the associated query's result set. The
 * row value is returned according to the query mode and the query transform.
 *
 * If no row count is given, a single row is returned. If a row count is given,
 * an array containing up to the `row_count` rows is returned. If `row_count` is
 * -1, all rows are returned. If the end of the result set has been reached,
 * `nil` is returned.
 *
 * If a block is given, rows are passed to the block and self is returned.
 *
 * @overload next()
 *   @return [any, Extralite::Query] next row or self if block is given
 * @overload next(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array<any>, Extralite::Query] next rows or self if block is given
 */
VALUE Query_next(int argc, VALUE *argv, VALUE self) {
  Query_t *query = self_to_query(self);
  rb_check_arity(argc, 0, 1);
  return Query_perform_next(self, MAX_ROWS_FROM_ARGV(argc, argv), query_impl(query->query_mode));
}

/* Returns all rows in the associated query's result set. Rows are returned
 * according to the query mode and the query transform.
 *
 * @return [Array<any>] all rows
 */
VALUE Query_to_a(VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, query_impl(query->query_mode));
}

/* Iterates through the result set, passing each row to the given block. If no
 * block is given, returns a `Extralite::Iterator` instance. Rows are given to
 * the block according to the query mode and the query transform.
 *
 * @return [Extralite::Query, Extralite::Iterator] self or an iterator if no block is given
 */
VALUE Query_each(VALUE self) {
  if (!rb_block_given_p()) return rb_funcall(cIterator, ID_new, 1, self);

  Query_t *query = self_to_query(self);
  query_reset(query);
  return Query_perform_next(self, ALL_ROWS, query_impl(query->query_mode));
}

/* call-seq:
 *   query.execute(*parameters) -> changes
 *
 * Runs a query returning the total changes effected. This method should be used
 * for data- or schema-manipulation queries.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     query = db.prepare('update foo set x = ? where y = ?')
 *     query.execute(42, 43)
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     query = db.prepare('update foo set x = :bar')
 *     query.execute(bar: 42)
 *     query.execute('bar' => 42)
 *     query.execute(':bar' => 42)
 */
VALUE Query_execute(int argc, VALUE *argv, VALUE self) {
  Query_t *query = self_to_query(self);
  query_reset_and_bind(query, argc, argv);
  return Query_perform_next(self, ALL_ROWS, safe_query_changes);
}

/* call-seq:
 *   query << [...] -> query
 *   query << { ... } -> query
 *
 * Runs the with the given parameters, returning the total changes effected.
 * This method should be used for data- or schema-manipulation queries.
 *
 * Query parameters to be bound to placeholders in the query can be specified as
 * a list of values or as a hash mapping parameter names to values. When
 * parameters are given as an array, the query should specify parameters using
 * `?`:
 *
 *     query = db.prepare('update foo set x = ? where y = ?')
 *     query << [42, 43]
 *
 * Named placeholders are specified using `:`. The placeholder values are
 * specified using a hash, where keys are either strings are symbols. String
 * keys can include or omit the `:` prefix. The following are equivalent:
 *
 *     query = db.prepare('update foo set x = :bar')
 *     query << { bar: 42 }
 *     query << { 'bar' => 42 }
 *     query << { ':bar' => 42 }
 */
VALUE Query_execute_chevrons(VALUE self, VALUE params) {
  Query_execute(1, &params, self);
  return self;
}

/* call-seq:
 *   query.batch_execute(params_array) -> changes
 *   query.batch_execute(enumerable) -> changes
 *   query.batch_execute(callable) -> changes
 *
 * Executes the query for each set of parameters in the paramter source. If an
 * enumerable is given, it is iterated and each of its values is used as the
 * parameters for running the query. If a callable is given, it is called
 * repeatedly and each of its return values is used as the parameters, until nil
 * is returned.
 * 
 * Returns the number of changes effected. This method is designed for inserting
 * multiple records.
 *
 *     query = db.prepare('insert into foo values (?, ?, ?)')
 *     records = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     query.batch_execute(records)
 * 
 *     source = [
 *       [1, 2, 3],
 *       [4, 5, 6]
 *     ]
 *     query.batch_execute { records.shift }
 *
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] array of parameters to run query with
 * @return [Integer] number of changes effected
 */
VALUE Query_batch_execute(VALUE self, VALUE parameters) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(DB_GVL_MODE(query), query->sqlite3_db, &query->stmt, query->sql);

  query_ctx ctx = QUERY_CTX(
    self,
    query->sql,
    query->db_struct,
    query->stmt,
    parameters,
    Qnil,
    QUERY_HASH,
    ROW_YIELD_OR_MODE(ROW_MULTI),
    ALL_ROWS
  );
  return safe_batch_execute(&ctx);
}

/* call-seq:
 *   query.batch_query(sql, params_array) -> rows
 *   query.batch_query(sql, enumerable) -> rows
 *   query.batch_query(sql, callable) -> rows
 *   query.batch_query(sql, params_array) { |rows| ... } -> changes
 *   query.batch_query(sql, enumerable) { |rows| ... } -> changes
 *   query.batch_query(sql, callable) { |rows| ... } -> changes
 *
 * Executes the prepared query for each list of parameters in the given paramter
 * source. If a block is given, it is called with the resulting rows for each
 * invocation of the query, and the total number of changes is returned.
 * Otherwise, an array containing the resulting rows for each invocation is
 * returned.
 * 
 * Rows are returned according to the query mode and transform.
 *
 *     q = db.prepare('insert into foo values (?, ?) returning bar, baz')
 *     records = [
 *       [1, 2],
 *       [3, 4]
 *     ]
 *     q.batch_query(records)
 *     #=> [{ bar: 1, baz: 2 }, { bar: 3, baz: 4}]
 * *
 * @param sql [String] query SQL
 * @param parameters [Array<Array, Hash>, Enumerable, Enumerator, Callable] parameters to run query with
 * @return [Array<Array>, Integer] Returned rows or total number of changes effected
 */
VALUE Query_batch_query(VALUE self, VALUE parameters) {
  Query_t *query = self_to_query(self);
  if (query->closed) rb_raise(cError, "Query is closed");

  if (!query->stmt)
    prepare_single_stmt(DB_GVL_MODE(query), query->sqlite3_db, &query->stmt, query->sql);

  query_ctx ctx = QUERY_CTX(
    self,
    query->sql,
    query->db_struct,
    query->stmt,
    parameters,
    query->transform_proc,
    query->query_mode,
    ROW_YIELD_OR_MODE(ROW_MULTI),
    ALL_ROWS
  );
  return safe_batch_query(&ctx);
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

/* call-seq:
 *   query.clone -> copy
 *   query.dup -> copy
 *
 * Returns a new query instance for the same SQL as the original query.
 *
 * @return [Extralite::Query] copy of query
 */
VALUE Query_clone(VALUE self) {
  Query_t *query = self_to_query(self);
  VALUE args[] = {
    query->db,
    query->sql,
    query_mode_to_symbol(query->query_mode)
  };
  return rb_funcall_with_block(cQuery, ID_new, 3, args, query->transform_proc);
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
    prepare_single_stmt(DB_GVL_MODE(query), query->sqlite3_db, &query->stmt, query->sql);

  int value = sqlite3_stmt_status(query->stmt, NUM2INT(op), RTEST(reset) ? 1 : 0);
  return INT2NUM(value);
}

/* Sets the transform block to the given block. If a transform block is set,
 * calls to #to_a, #next, #each and #batch_query will transform values fetched
 * from the database using the transform block before passing them to the
 * application code. To remove the transform block, call `#transform`
 * without a block. The transform for each row is done by passing the row hash
 * to the block.
 *
 *     # fetch column c as an ORM object
 *     q = db.prepare('select * from foo order by a').transform do |h|
 *       MyModel.new(h)
 *     end
 *
 * @return [Extralite::Query] query
 */
VALUE Query_transform(VALUE self) {
  Query_t *query = self_to_query(self);

  RB_OBJ_WRITE(self, &query->transform_proc, rb_block_given_p() ? rb_block_proc() : Qnil);
  return self;
}

/* Returns a short string representation of the query instance, including the
 * SQL string.
 *
 * @return [String] string representation
 */
VALUE Query_inspect(VALUE self) {
  VALUE cname = rb_class_name(CLASS_OF(self));
  VALUE sql = self_to_query(self)->sql;
  if (RSTRING_LEN(sql) > 48) {
    sql = rb_funcall(sql, ID_slice, 2, INT2FIX(0), INT2FIX(45));
    rb_str_cat2(sql, "...");
  }
  sql = rb_funcall(sql, ID_inspect, 0);

  RB_GC_GUARD(sql);
  return rb_sprintf("#<%"PRIsVALUE":%p %"PRIsVALUE">", cname, (void*)self, sql);
}

/* Returns the query mode.
 *
 * @return [Symbol] query mode
 */
VALUE Query_mode_get(VALUE self) {
  Query_t *query = self_to_query(self);
  return query_mode_to_symbol(query->query_mode);
}

/* call-seq:
 *   query.mode = mode
 * 
 * Sets the query mode. This can be one of `:hash`, `:argv`, `:ary`.
 *
 * @param mode [Symbol] query mode
 * @return [Symbol] query mode
 */
VALUE Query_mode_set(VALUE self, VALUE mode) {
  Query_t *query = self_to_query(self);
  query->query_mode = symbol_to_query_mode(mode);
  return mode;
}

void Init_ExtraliteQuery(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cQuery = rb_define_class_under(mExtralite, "Query", rb_cObject);
  rb_define_alloc_func(cQuery, Query_allocate);

  rb_define_method(cQuery, "bind",           Query_bind, -1);
  rb_define_method(cQuery, "close",          Query_close, 0);
  rb_define_method(cQuery, "closed?",        Query_closed_p, 0);
  rb_define_method(cQuery, "columns",        Query_columns, 0);
  rb_define_method(cQuery, "clone",          Query_clone, 0);
  rb_define_method(cQuery, "database",       Query_database, 0);
  rb_define_method(cQuery, "db",             Query_database, 0);
  rb_define_method(cQuery, "dup",            Query_clone, 0);
  rb_define_method(cQuery, "each",           Query_each, 0);
  rb_define_method(cQuery, "eof?",           Query_eof_p, 0);
  rb_define_method(cQuery, "execute",        Query_execute, -1);
  rb_define_method(cQuery, "<<",             Query_execute_chevrons, 1);
  rb_define_method(cQuery, "batch_execute",  Query_batch_execute, 1);
  rb_define_method(cQuery, "batch_query",    Query_batch_query, 1);
  rb_define_method(cQuery, "initialize",     Query_initialize, 3);
  rb_define_method(cQuery, "inspect",        Query_inspect, 0);
  rb_define_method(cQuery, "mode",           Query_mode_get, 0);
  rb_define_method(cQuery, "mode=",          Query_mode_set, 1);
  rb_define_method(cQuery, "next",           Query_next, -1);
  rb_define_method(cQuery, "reset",          Query_reset, 0);
  rb_define_method(cQuery, "sql",            Query_sql, 0);
  rb_define_method(cQuery, "status",         Query_status, -1);
  rb_define_method(cQuery, "to_a",           Query_to_a, 0);
  rb_define_method(cQuery, "transform",      Query_transform, 0);

  ID_inspect  = rb_intern("inspect");
  ID_slice    = rb_intern("slice");

  SYM_hash          = ID2SYM(rb_intern("hash"));
  SYM_argv          = ID2SYM(rb_intern("argv"));
  SYM_ary           = ID2SYM(rb_intern("ary"));

  rb_gc_register_mark_object(SYM_hash);
  rb_gc_register_mark_object(SYM_argv);
  rb_gc_register_mark_object(SYM_ary);
}
