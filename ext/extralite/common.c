#include <stdio.h>
#include "extralite.h"

static inline VALUE get_column_value(sqlite3_stmt *stmt, int col, int type) {
  switch (type) {
    case SQLITE_NULL:
      return Qnil;
    case SQLITE_INTEGER:
      return LL2NUM(sqlite3_column_int64(stmt, col));
    case SQLITE_FLOAT:
      return DBL2NUM(sqlite3_column_double(stmt, col));
    case SQLITE_TEXT:
      return rb_str_new_cstr((char *)sqlite3_column_text(stmt, col));
    case SQLITE_BLOB:
      return rb_str_new((const char *)sqlite3_column_blob(stmt, col), (long)sqlite3_column_bytes(stmt, col));
    default:
      rb_raise(cError, "Unknown column type: %d", type);
  }

  return Qnil;
}

void bind_parameter_value(sqlite3_stmt *stmt, int pos, VALUE value);

void bind_hash_parameter_values(sqlite3_stmt *stmt, VALUE hash) {
  VALUE keys = rb_funcall(hash, ID_KEYS, 0);
  long len = RARRAY_LEN(keys);
  for (long i = 0; i < len; i++) {
    VALUE k = RARRAY_AREF(keys, i);
    VALUE v = rb_hash_aref(hash, k);

    switch (TYPE(k)) {
      case T_FIXNUM:
        bind_parameter_value(stmt, FIX2INT(k), v);
        break;
      case T_SYMBOL:
        k = rb_funcall(k, ID_TO_S, 0);
      case T_STRING:
        if(RSTRING_PTR(k)[0] != ':') k = rb_str_plus(rb_str_new2(":"), k);
        int pos = sqlite3_bind_parameter_index(stmt, StringValuePtr(k));
        bind_parameter_value(stmt, pos, v);
        break;
      default:
        rb_raise(cError, "Cannot bind hash key value idx %ld", i);
    }
  }
  RB_GC_GUARD(keys);
}

inline void bind_parameter_value(sqlite3_stmt *stmt, int pos, VALUE value) {
  switch (TYPE(value)) {
    case T_NIL:
      sqlite3_bind_null(stmt, pos);
      return;
    case T_FIXNUM:
      sqlite3_bind_int64(stmt, pos, NUM2LL(value));
      return;
    case T_FLOAT:
      sqlite3_bind_double(stmt, pos, NUM2DBL(value));
      return;
    case T_TRUE:
      sqlite3_bind_int(stmt, pos, 1);
      return;
    case T_FALSE:
      sqlite3_bind_int(stmt, pos, 0);
      return;
    case T_STRING:
      sqlite3_bind_text(stmt, pos, RSTRING_PTR(value), RSTRING_LEN(value), SQLITE_TRANSIENT);
      return;
    case T_HASH:
      bind_hash_parameter_values(stmt, value);
      return;
    default:
      rb_raise(cError, "Cannot bind parameter at position %d", pos);
  }
}

void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv) {
  for (int i = 0; i < argc; i++) {
    bind_parameter_value(stmt, i + 1, argv[i]);
  }
}

void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE obj) {
  if (TYPE(obj) == T_ARRAY) {
    int count = RARRAY_LEN(obj);
    for (int i = 0; i < count; i++)
      bind_parameter_value(stmt, i + 1, RARRAY_AREF(obj, i));
  }
  else
    bind_parameter_value(stmt, 1, obj);
}

static inline VALUE get_column_names(sqlite3_stmt *stmt, int column_count) {
  VALUE arr = rb_ary_new2(column_count);
  for (int i = 0; i < column_count; i++) {
    VALUE name = ID2SYM(rb_intern(sqlite3_column_name(stmt, i)));
    rb_ary_push(arr, name);
  }
  return arr;
}

static inline VALUE row_to_hash(sqlite3_stmt *stmt, int column_count, VALUE column_names) {
  VALUE row = rb_hash_new();
  for (int i = 0; i < column_count; i++) {
    VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
    rb_hash_aset(row, RARRAY_AREF(column_names, i), value);
  }
  return row;
}

static inline VALUE row_to_ary(sqlite3_stmt *stmt, int column_count) {
  VALUE row = rb_ary_new2(column_count);
  for (int i = 0; i < column_count; i++) {
    VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
    rb_ary_push(row, value);
  }
  return row;
}

typedef struct {
  sqlite3 *db;
  sqlite3_stmt **stmt;
  const char *str;
  long len;
  int rc;
} prepare_stmt_ctx;

void *prepare_multi_stmt_without_gvl(void *ptr) {
  prepare_stmt_ctx *ctx = (prepare_stmt_ctx *)ptr;
  const char *rest = NULL;
  const char *str = ctx->str;
  const char *end = ctx->str + ctx->len;
  while (1) {
    ctx->rc = sqlite3_prepare_v2(ctx->db, str, end - str, ctx->stmt, &rest);
    if (ctx->rc) {
      // error
      sqlite3_finalize(*ctx->stmt);
      return NULL;
    }

    if (rest == end) return NULL;

    // perform current query, but discard its results
    ctx->rc = sqlite3_step(*ctx->stmt);
    sqlite3_finalize(*ctx->stmt);
    switch (ctx->rc) {
    case SQLITE_BUSY:
    case SQLITE_ERROR:
    case SQLITE_MISUSE:
      return NULL;
    }
    str = rest;
  }
  return NULL;
}

/*
This function prepares a statement from an SQL string containing one or more SQL
statements. It will release the GVL while the statements are being prepared and
executed. All statements excluding the last one are executed. The last statement
is not executed, but instead handed back to the caller for looping over results.
*/
void prepare_multi_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql) {
  prepare_stmt_ctx ctx = {db, stmt, RSTRING_PTR(sql), RSTRING_LEN(sql), 0};
  rb_thread_call_without_gvl(prepare_multi_stmt_without_gvl, (void *)&ctx, RUBY_UBF_IO, 0);
  RB_GC_GUARD(sql);

  switch (ctx.rc) {
  case 0:
    return;
  case SQLITE_BUSY:
    rb_raise(cBusyError, "Database is busy");
  case SQLITE_ERROR:
    rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
  default:
    rb_raise(cError, "Invalid return code for prepare_multi_stmt_without_gvl: %d (please open an issue on https://github.com/digital-fabric/extralite)", ctx.rc);
  }
}

#define SQLITE_MULTI_STMT -1

void *prepare_single_stmt_without_gvl(void *ptr) {
  prepare_stmt_ctx *ctx = (prepare_stmt_ctx *)ptr;
  const char *rest = NULL;
  const char *str = ctx->str;
  const char *end = ctx->str + ctx->len;

  ctx->rc = sqlite3_prepare_v2(ctx->db, str, end - str, ctx->stmt, &rest);
  if (ctx->rc)
    goto discard_stmt;
  else if (rest != end) {
    ctx->rc = SQLITE_MULTI_STMT;
    goto discard_stmt;
  }
  goto end;
discard_stmt:
  if (*ctx->stmt) {
    sqlite3_finalize(*ctx->stmt);
    *ctx->stmt = NULL;
  }
end:
  return NULL;
}

void prepare_single_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql) {
  prepare_stmt_ctx ctx = {db, stmt, RSTRING_PTR(sql), RSTRING_LEN(sql), 0};
  rb_thread_call_without_gvl(prepare_single_stmt_without_gvl, (void *)&ctx, RUBY_UBF_IO, 0);
  RB_GC_GUARD(sql);

  switch (ctx.rc) {
  case 0:
    return;
  case SQLITE_BUSY:
    rb_raise(cBusyError, "Database is busy");
  case SQLITE_ERROR:
    rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
  case SQLITE_MULTI_STMT:
    rb_raise(cSQLError, "A prepared statement does not accept SQL strings with multiple queries");
  default:
    rb_raise(cError, "Invalid return code for prepare_multi_stmt_without_gvl: %d (please open an issue on https://github.com/digital-fabric/extralite)", ctx.rc);
  }
}

struct step_ctx {
  sqlite3_stmt *stmt;
  int rc;
};

void *stmt_iterate_without_gvl(void *ptr) {
  struct step_ctx *ctx = (struct step_ctx *)ptr;
  ctx->rc = sqlite3_step(ctx->stmt);
  return NULL;
}

int stmt_iterate(sqlite3_stmt *stmt, sqlite3 *db) {
  struct step_ctx ctx = {stmt, 0};
  rb_thread_call_without_gvl(stmt_iterate_without_gvl, (void *)&ctx, RUBY_UBF_IO, 0);
  switch (ctx.rc) {
    case SQLITE_ROW:
      return 1;
    case SQLITE_DONE:
      return 0;
    case SQLITE_BUSY:
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_INTERRUPT:
      rb_raise(cInterruptError, "Query was interrupted");
    case SQLITE_ERROR:
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
    default:
      rb_raise(cError, "Invalid return code for sqlite3_step: %d (please open an issue on https://github.com/digital-fabric/extralite)", ctx.rc);
  }

  return 0;
}

VALUE cleanup_stmt(query_ctx *ctx) {
  if (ctx->stmt) sqlite3_finalize(ctx->stmt);
  return Qnil;
}

VALUE safe_query_hash(query_ctx *ctx) {
  VALUE result = ctx->self;
  int yield_to_block = rb_block_given_p();
  VALUE row;
  int column_count;
  VALUE column_names;

  column_count = sqlite3_column_count(ctx->stmt);
  column_names = get_column_names(ctx->stmt, column_count);

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();

  while (stmt_iterate(ctx->stmt, ctx->sqlite3_db)) {
    row = row_to_hash(ctx->stmt, column_count, column_names);
    if (yield_to_block) rb_yield(row);
    else                rb_ary_push(result, row);
  }

  RB_GC_GUARD(column_names);
  RB_GC_GUARD(row);
  RB_GC_GUARD(result);
  return result;
}

VALUE safe_query_ary(query_ctx *ctx) {
  int column_count;
  VALUE result = ctx->self;
  int yield_to_block = rb_block_given_p();
  VALUE row;

  column_count = sqlite3_column_count(ctx->stmt);

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();

  while (stmt_iterate(ctx->stmt, ctx->sqlite3_db)) {
    row = row_to_ary(ctx->stmt, column_count);
    if (yield_to_block) rb_yield(row);
    else                rb_ary_push(result, row);
  }

  RB_GC_GUARD(row);
  RB_GC_GUARD(result);
  return result;
}

VALUE safe_query_single_row(query_ctx *ctx) {
  int column_count;
  VALUE row = Qnil;
  VALUE column_names;

  column_count = sqlite3_column_count(ctx->stmt);
  column_names = get_column_names(ctx->stmt, column_count);

  if (stmt_iterate(ctx->stmt, ctx->sqlite3_db))
    row = row_to_hash(ctx->stmt, column_count, column_names);

  RB_GC_GUARD(row);
  RB_GC_GUARD(column_names);
  return row;
}

VALUE safe_query_single_column(query_ctx *ctx) {
  int column_count;
  VALUE result = ctx->self;
  int yield_to_block = rb_block_given_p();
  VALUE value;

  column_count = sqlite3_column_count(ctx->stmt);
  if (column_count != 1)
    rb_raise(cError, "Expected query result to have 1 column");

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();

  while (stmt_iterate(ctx->stmt, ctx->sqlite3_db)) {
    value = get_column_value(ctx->stmt, 0, sqlite3_column_type(ctx->stmt, 0));
    if (yield_to_block) rb_yield(value); else rb_ary_push(result, value);
  }

  RB_GC_GUARD(value);
  RB_GC_GUARD(result);
  return result;
}

VALUE safe_query_single_value(query_ctx *ctx) {
  int column_count;
  VALUE value = Qnil;

  column_count = sqlite3_column_count(ctx->stmt);
  if (column_count != 1)
    rb_raise(cError, "Expected query result to have 1 column");

  if (stmt_iterate(ctx->stmt, ctx->sqlite3_db))
    value = get_column_value(ctx->stmt, 0, sqlite3_column_type(ctx->stmt, 0));

  RB_GC_GUARD(value);
  return value;
}

VALUE safe_execute_multi(query_ctx *ctx) {
  int count = RARRAY_LEN(ctx->params);
  int changes = 0;

  for (int i = 0; i < count; i++) {
    sqlite3_reset(ctx->stmt);
    sqlite3_clear_bindings(ctx->stmt);
    bind_all_parameters_from_object(ctx->stmt, RARRAY_AREF(ctx->params, i));

    while (stmt_iterate(ctx->stmt, ctx->sqlite3_db));
    changes += sqlite3_changes(ctx->sqlite3_db);
  }

  return INT2FIX(changes);
}

VALUE safe_query_columns(query_ctx *ctx) {
  return get_column_names(ctx->stmt, sqlite3_column_count(ctx->stmt));
}
