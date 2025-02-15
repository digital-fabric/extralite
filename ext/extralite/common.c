#include <stdio.h>
#include "extralite.h"

rb_encoding *UTF8_ENCODING;

inline void *gvl_call(enum gvl_mode mode, void *(*fn)(void *), void *data) {
  switch (mode) {
    case GVL_RELEASE:
      return rb_thread_call_without_gvl(fn, data, RUBY_UBF_IO, 0);
    default:
      return fn(data);
  }
}

static inline VALUE get_column_value(sqlite3_stmt *stmt, int col, int type) {
  switch (type) {
    case SQLITE_NULL:
      return Qnil;
    case SQLITE_INTEGER:
      return LL2NUM(sqlite3_column_int64(stmt, col));
    case SQLITE_FLOAT:
      return DBL2NUM(sqlite3_column_double(stmt, col));
    case SQLITE_TEXT:
      return rb_enc_str_new((char *)sqlite3_column_text(stmt, col), (long)sqlite3_column_bytes(stmt, col), UTF8_ENCODING);
    case SQLITE_BLOB:
      return rb_str_new((const char *)sqlite3_column_blob(stmt, col), (long)sqlite3_column_bytes(stmt, col));
    default:
      rb_raise(cError, "Unknown column type: %d", type);
  }

  return Qnil;
}

int bind_parameter_value(sqlite3_stmt *stmt, int pos, VALUE value);

static inline void bind_key_value(sqlite3_stmt *stmt, VALUE k, VALUE v) {
  switch (TYPE(k)) {
    case T_FIXNUM:
      bind_parameter_value(stmt, FIX2INT(k), v);
      break;
    case T_SYMBOL:
      k = rb_sym2str(k);
    case T_STRING:
      if (RSTRING_PTR(k)[0] != ':') k = rb_str_plus(rb_str_new2(":"), k);
      int pos = sqlite3_bind_parameter_index(stmt, StringValuePtr(k));
      bind_parameter_value(stmt, pos, v);
      break;
    default:
      rb_raise(cParameterError, "Cannot bind parameter with a key of type %"PRIsVALUE"",
        rb_class_name(rb_obj_class(k)));
  }
}

void bind_hash_parameter_values(sqlite3_stmt *stmt, VALUE hash) {
  VALUE keys = rb_funcall(hash, ID_keys, 0);
  long len = RARRAY_LEN(keys);
  for (long i = 0; i < len; i++) {
    VALUE k = RARRAY_AREF(keys, i);
    VALUE v = rb_hash_aref(hash, k);
    bind_key_value(stmt, k, v);
  }
  RB_GC_GUARD(keys);
}

void bind_struct_parameter_values(sqlite3_stmt *stmt, VALUE struct_obj) {
  VALUE members = rb_struct_members(struct_obj);
  for (long i = 0; i < RSTRUCT_LEN(struct_obj); i++) {
    VALUE k = rb_ary_entry(members, i);
    VALUE v = RSTRUCT_GET(struct_obj, i);
    bind_key_value(stmt, k, v);
  }
  RB_GC_GUARD(members);
}

inline int bind_parameter_value(sqlite3_stmt *stmt, int pos, VALUE value) {
  switch (TYPE(value)) {
    case T_NIL:
      sqlite3_bind_null(stmt, pos);
      return 1;
    case T_FIXNUM:
    case T_BIGNUM:
      sqlite3_bind_int64(stmt, pos, NUM2LL(value));
      return 1;
    case T_FLOAT:
      sqlite3_bind_double(stmt, pos, NUM2DBL(value));
      return 1;
    case T_TRUE:
      sqlite3_bind_int(stmt, pos, 1);
      return 1;
    case T_FALSE:
      sqlite3_bind_int(stmt, pos, 0);
      return 1;
    case T_SYMBOL:
      value = rb_sym2str(value);
    case T_STRING:
      if (rb_enc_get_index(value) == rb_ascii8bit_encindex() || CLASS_OF(value) == cBlob)
        sqlite3_bind_blob(stmt, pos, RSTRING_PTR(value), RSTRING_LEN(value), SQLITE_TRANSIENT);
      else
        sqlite3_bind_text(stmt, pos, RSTRING_PTR(value), RSTRING_LEN(value), SQLITE_TRANSIENT);
      return 1;
    case T_ARRAY:
      {
        int count = RARRAY_LEN(value);
        for (int i = 0; i < count; i++)
          bind_parameter_value(stmt, pos + i, RARRAY_AREF(value, i));
        return count;
      }
    case T_HASH:
      bind_hash_parameter_values(stmt, value);
      return 0;
    case T_STRUCT:
      bind_struct_parameter_values(stmt, value);
      return 0;
    default:
      rb_raise(cParameterError, "Cannot bind parameter at position %d of type %"PRIsVALUE"",
        pos, rb_class_name(rb_obj_class(value)));
  }
}

inline void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv) {
  int pos = 1;
  for (int i = 0; i < argc; i++) {
    pos += bind_parameter_value(stmt, pos, argv[i]);
  }
}

inline void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE obj) {
  if (TYPE(obj) == T_ARRAY) {
    int pos = 1;
    int count = RARRAY_LEN(obj);
    for (int i = 0; i < count; i++)
      pos += bind_parameter_value(stmt, pos, RARRAY_AREF(obj, i));
  }
  else
    bind_parameter_value(stmt, 1, obj);
}

#define MAX_EMBEDDED_COLUMN_NAMES 12

struct column_names {
  int count;
  union {
    VALUE array;
    VALUE names[MAX_EMBEDDED_COLUMN_NAMES];
  };
};

static inline void column_names_setup(struct column_names *names, int count) {
  names->count = count;
  names->array = (count > MAX_EMBEDDED_COLUMN_NAMES) ? rb_ary_new2(count) : Qnil;
}

static inline void column_names_set(struct column_names *names, int idx, VALUE value) {
  if (names->count <= MAX_EMBEDDED_COLUMN_NAMES) 
    names->names[idx] = value;
  else
    rb_ary_push(names->array, value);
}

static inline VALUE column_names_get(struct column_names *names, int idx) {
  return (names->count <= MAX_EMBEDDED_COLUMN_NAMES) ?
    names->names[idx] : RARRAY_AREF(names->array, idx);
}

static inline struct column_names get_column_names(sqlite3_stmt *stmt, int column_count) {
  struct column_names names;
  column_names_setup(&names, column_count);
  for (int i = 0; i < column_count; i++) {
    VALUE name = ID2SYM(rb_intern(sqlite3_column_name(stmt, i)));
    column_names_set(&names, i, name);
  }
  return names;
}

static inline VALUE get_column_names_array(sqlite3_stmt *stmt, int column_count) {
  VALUE arr = rb_ary_new2(column_count);
  for (int i = 0; i < column_count; i++) {
    VALUE name = ID2SYM(rb_intern(sqlite3_column_name(stmt, i)));
    rb_ary_push(arr, name);
  }
  return arr;
}

static inline VALUE row_to_hash(sqlite3_stmt *stmt, int column_count, struct column_names *names) {
  VALUE row = rb_hash_new();
  if (names->count <= MAX_EMBEDDED_COLUMN_NAMES) {
    for (int i = 0; i < column_count; i++) {
      VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
      rb_hash_aset(row, names->names[i], value);
    }
  }
  else {
    for (int i = 0; i < column_count; i++) {
      VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
      rb_hash_aset(row, RARRAY_AREF(names->array, i), value);
    }
  }
  return row;
}

static inline VALUE row_to_array(sqlite3_stmt *stmt, int column_count) {
  VALUE row = rb_ary_new2(column_count);
  for (int i = 0; i < column_count; i++) {
    VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
    rb_ary_push(row, value);
  }
  return row;
}

static inline void row_to_splat_values(sqlite3_stmt *stmt, int column_count, VALUE *values) {
  for (int i = 0; i < column_count; i++) {
    values[i] = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
  }
}

typedef struct {
  sqlite3 *db;
  sqlite3_stmt **stmt;
  const char *str;
  long len;
  int rc;
} prepare_stmt_ctx;

void *prepare_multi_stmt_impl(void *ptr) {
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
void prepare_multi_stmt(enum gvl_mode mode, sqlite3 *db, sqlite3_stmt **stmt, VALUE sql) {
  prepare_stmt_ctx ctx = {db, stmt, RSTRING_PTR(sql), RSTRING_LEN(sql), 0};
  gvl_call(mode, prepare_multi_stmt_impl, (void *)&ctx);
  RB_GC_GUARD(sql);

  switch (ctx.rc) {
  case 0:
    return;
  case SQLITE_BUSY:
    rb_raise(cBusyError, "Database is busy");
  case SQLITE_ERROR:
    rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
  default:
    rb_raise(cError, "%s", sqlite3_errmsg(db));
  }
}

#define SQLITE_MULTI_STMT -1

void *prepare_single_stmt_impl(void *ptr) {
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

void prepare_single_stmt(enum gvl_mode mode, sqlite3 *db, sqlite3_stmt **stmt, VALUE sql) {
  prepare_stmt_ctx ctx = {db, stmt, RSTRING_PTR(sql), RSTRING_LEN(sql), 0};
  gvl_call(mode, prepare_single_stmt_impl, (void *)&ctx);
  RB_GC_GUARD(sql);

  switch (ctx.rc) {
  case 0:
    return;
  case SQLITE_BUSY:
    rb_raise(cBusyError, "Database is busy");
  case SQLITE_ERROR:
    rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
  case SQLITE_MULTI_STMT:
    rb_raise(cError, "A prepared statement does not accept SQL strings with multiple queries");
  default:
    rb_raise(cError, "%s", sqlite3_errmsg(db));
  }
}

struct step_ctx {
  sqlite3_stmt *stmt;
  int rc;
};

void *stmt_iterate_step(void *ptr) {
  struct step_ctx *ctx = (struct step_ctx *)ptr;
  ctx->rc = sqlite3_step(ctx->stmt);
  return NULL;
}

inline enum gvl_mode stepwise_gvl_mode(query_ctx *ctx) {
  // a negative or zero threshold means the GVL is always held during iteration.
  if (ctx->gvl_release_threshold <= 0) return GVL_HOLD;
  
  if (!sqlite3_stmt_busy(ctx->stmt)) return GVL_RELEASE;

  // if positive, the GVL is normally held, and release every <threshold> steps.
  return (ctx->step_count % ctx->gvl_release_threshold) ? GVL_HOLD : GVL_RELEASE;
}

inline int stmt_iterate(query_ctx *ctx) {
  struct step_ctx step_ctx = {ctx->stmt, 0};
  ctx->step_count += 1;
  gvl_call(stepwise_gvl_mode(ctx), stmt_iterate_step, (void *)&step_ctx);
  switch (step_ctx.rc) {
    case SQLITE_ROW:
      return 1;
    case SQLITE_DONE:
      ctx->eof = 1;
      return 0;
    case SQLITE_BUSY:
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_INTERRUPT:
      rb_raise(cInterruptError, "Query was interrupted");
    case SQLITE_ERROR:
      rb_raise(cSQLError, "%s", sqlite3_errmsg(ctx->sqlite3_db));
    default:
      rb_raise(cError, "%s", sqlite3_errmsg(ctx->sqlite3_db));
  }

  return 0;
}

VALUE cleanup_stmt(query_ctx *ctx) {
  if (ctx->stmt) sqlite3_finalize(ctx->stmt);
  return Qnil;
}

VALUE safe_query_splat(query_ctx *ctx);

VALUE safe_query_hash(query_ctx *ctx) {
  VALUE array = ROW_MULTI_P(ctx->row_mode) ? rb_ary_new() : Qnil;
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  struct column_names names = get_column_names(ctx->stmt, column_count);
  int row_count = 0;
  int do_transform = !NIL_P(ctx->transform_proc);

  while (stmt_iterate(ctx)) {
    row = row_to_hash(ctx->stmt, column_count, &names);
    if (do_transform)
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
    row_count++;
    switch (ctx->row_mode) {
      case ROW_YIELD:
        rb_yield(row);
        break;
      case ROW_MULTI:
        rb_ary_push(array, row);
        break;
      case ROW_SINGLE:
        return row;
    }
    if (ctx->max_rows != ALL_ROWS && row_count >= ctx->max_rows)
      return ROW_MULTI_P(ctx->row_mode) ? array : ctx->self;
  }

  RB_GC_GUARD(names.array);
  RB_GC_GUARD(row);
  RB_GC_GUARD(array);
  return ROW_MULTI_P(ctx->row_mode) ? array : Qnil;
}

#define MAX_ARGV_COLUMNS 8
#define NIL_ARGV_VALUES {Qnil, Qnil, Qnil, Qnil, Qnil, Qnil, Qnil, Qnil}
#define ARGV_GC_GUARD(values) \
  RB_GC_GUARD(values[0]); \
  RB_GC_GUARD(values[1]); \
  RB_GC_GUARD(values[2]); \
  RB_GC_GUARD(values[3]); \
  RB_GC_GUARD(values[4]); \
  RB_GC_GUARD(values[5]); \
  RB_GC_GUARD(values[6]); \
  RB_GC_GUARD(values[7])

#define ARGV_GET_ROW(ctx, column_count, argv_values, row, do_transform, return_rows) \
  row_to_splat_values(ctx->stmt, column_count, argv_values); \
  if (do_transform) \
    row = rb_funcall2(ctx->transform_proc, ID_call, column_count, argv_values); \
  else if (return_rows) \
    row = column_count == 1 ? argv_values[0] : rb_ary_new_from_values(column_count, argv_values);

VALUE safe_query_splat(query_ctx *ctx) {
  VALUE array = ROW_MULTI_P(ctx->row_mode) ? rb_ary_new() : Qnil;
  VALUE argv_values[MAX_ARGV_COLUMNS] = NIL_ARGV_VALUES;
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  if (column_count > MAX_ARGV_COLUMNS)
    rb_raise(cError, "Conversion is supported only up to %d columns", MAX_ARGV_COLUMNS);

  int do_transform = !NIL_P(ctx->transform_proc);
  int return_rows = (ctx->row_mode != ROW_YIELD);

  int row_count = 0;
  while (stmt_iterate(ctx)) {
    row_count++;
    ARGV_GET_ROW(ctx, column_count, argv_values, row, do_transform, return_rows);
    switch (ctx->row_mode) {
      case ROW_YIELD:
        if (do_transform)
          rb_yield(row);
        else
          rb_yield_values2(column_count, argv_values);
        break;
      case ROW_MULTI:
        rb_ary_push(array, row);
        break;
      case ROW_SINGLE:
        return row;
    }
    if (ctx->max_rows != ALL_ROWS && row_count >= ctx->max_rows)
      return ROW_MULTI_P(ctx->row_mode) ? array : ctx->self;
  }

  ARGV_GC_GUARD(argv_values);
  RB_GC_GUARD(row);
  RB_GC_GUARD(array);
  return ROW_MULTI_P(ctx->row_mode) ? array : Qnil;
}

VALUE safe_query_array(query_ctx *ctx) {
  VALUE array = ROW_MULTI_P(ctx->row_mode) ? rb_ary_new() : Qnil;
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  int row_count = 0;
  int do_transform = !NIL_P(ctx->transform_proc);

  while (stmt_iterate(ctx)) {
    row = row_to_array(ctx->stmt, column_count);
    if (do_transform)
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
    row_count++;
    switch (ctx->row_mode) {
      case ROW_YIELD:
        rb_yield(row);
        break;
      case ROW_MULTI:
        rb_ary_push(array, row);
        break;
      case ROW_SINGLE:
        return row;
    }
    if (ctx->max_rows != ALL_ROWS && row_count >= ctx->max_rows)
      return ROW_MULTI_P(ctx->row_mode) ? array : ctx->self;
  }

  RB_GC_GUARD(row);
  RB_GC_GUARD(array);
  return ROW_MULTI_P(ctx->row_mode) ? array : Qnil;
}

VALUE safe_query_single_row_hash(query_ctx *ctx) {
  int column_count = sqlite3_column_count(ctx->stmt);
  VALUE row = Qnil;
  struct column_names names = get_column_names(ctx->stmt, column_count);

  if (stmt_iterate(ctx)) {
    row = row_to_hash(ctx->stmt, column_count, &names);
    if (!NIL_P(ctx->transform_proc))
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
  }

  RB_GC_GUARD(row);
  RB_GC_GUARD(names.array);
  return row;
}

VALUE safe_query_single_row_splat(query_ctx *ctx) {
  VALUE argv_values[MAX_ARGV_COLUMNS] = NIL_ARGV_VALUES;
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  if (column_count > MAX_ARGV_COLUMNS)
    rb_raise(cError, "Conversion is supported only up to %d columns", MAX_ARGV_COLUMNS);
  int do_transform = !NIL_P(ctx->transform_proc);

  if (stmt_iterate(ctx)) {
    ARGV_GET_ROW(ctx, column_count, argv_values, row, do_transform, 1);
  }

  ARGV_GC_GUARD(argv_values);
  RB_GC_GUARD(row);
  return row;
}

VALUE safe_query_single_row_array(query_ctx *ctx) {
  int column_count = sqlite3_column_count(ctx->stmt);
  VALUE row = Qnil;
  int do_transform = !NIL_P(ctx->transform_proc);

  if (stmt_iterate(ctx)) {
    row = row_to_array(ctx->stmt, column_count);
    if (do_transform)
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
  }

  RB_GC_GUARD(row);
  return row;
}

enum batch_mode {
  BATCH_EXECUTE,
  BATCH_QUERY_HASH,
  BATCH_QUERY_SPLAT,
  BATCH_QUERY_ARRAY,
};

static inline VALUE batch_iterate_hash(query_ctx *ctx) {
  VALUE rows = rb_ary_new();
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  struct column_names names = get_column_names(ctx->stmt, column_count);
  const int do_transform = !NIL_P(ctx->transform_proc);

  while (stmt_iterate(ctx)) {
    row = row_to_hash(ctx->stmt, column_count, &names);
    if (do_transform)
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
    rb_ary_push(rows, row);
  }

  RB_GC_GUARD(names.array);
  RB_GC_GUARD(row);
  RB_GC_GUARD(rows);
  return rows;
}

static inline VALUE batch_iterate_array(query_ctx *ctx) {
  VALUE rows = rb_ary_new();
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  int do_transform = !NIL_P(ctx->transform_proc);

  while (stmt_iterate(ctx)) {
    row = row_to_array(ctx->stmt, column_count);
    if (do_transform)
      row = rb_funcall(ctx->transform_proc, ID_call, 1, row);
    rb_ary_push(rows, row);
  }

  RB_GC_GUARD(row);
  RB_GC_GUARD(rows);
  return rows;
}

static inline VALUE batch_iterate_splat(query_ctx *ctx) {
  VALUE rows = rb_ary_new();
  VALUE argv_values[MAX_ARGV_COLUMNS] = NIL_ARGV_VALUES;
  VALUE row = Qnil;
  int column_count = sqlite3_column_count(ctx->stmt);
  if (column_count > MAX_ARGV_COLUMNS)
    rb_raise(cError, "Conversion is supported only up to %d columns", MAX_ARGV_COLUMNS);
  int do_transform = !NIL_P(ctx->transform_proc);

  while (stmt_iterate(ctx)) {
    ARGV_GET_ROW(ctx, column_count, argv_values, row, do_transform, 1);
    rb_ary_push(rows, row);
  }

  ARGV_GC_GUARD(argv_values);
  RB_GC_GUARD(row);
  RB_GC_GUARD(rows);
  return rows;
}

static inline void batch_iterate(query_ctx *ctx, enum batch_mode mode, VALUE *rows) {
  switch (mode) {
    case BATCH_EXECUTE:
      while (stmt_iterate(ctx));
      break;
    case BATCH_QUERY_HASH:
      *rows = batch_iterate_hash(ctx);
      break;
    case BATCH_QUERY_SPLAT:
      *rows = batch_iterate_splat(ctx);
      break;
    case BATCH_QUERY_ARRAY:
      *rows = batch_iterate_array(ctx);
      break;
  }
}

static inline VALUE batch_run_array(query_ctx *ctx, enum batch_mode batch_mode) {
  int count = RARRAY_LEN(ctx->params);
  int block_given = rb_block_given_p();
  VALUE results = (batch_mode != BATCH_EXECUTE) && !block_given ? rb_ary_new() : Qnil;
  VALUE rows = Qnil;
  int changes = 0;

  for (int i = 0; i < count; i++) {
    sqlite3_reset(ctx->stmt);
    sqlite3_clear_bindings(ctx->stmt);
    Database_issue_query(ctx->db, ctx->sql);
    bind_all_parameters_from_object(ctx->stmt, RARRAY_AREF(ctx->params, i));

    batch_iterate(ctx, batch_mode, &rows);
    changes += sqlite3_changes(ctx->sqlite3_db);

    if (batch_mode != BATCH_EXECUTE) {
      if (block_given)
        rb_yield(rows);
      else
        rb_ary_push(results, rows);
    }
  }

  RB_GC_GUARD(rows);
  RB_GC_GUARD(results);

  if (batch_mode == BATCH_EXECUTE || block_given)
    return INT2FIX(changes);
  else
    return results;
}

struct batch_execute_each_ctx {
  query_ctx *ctx;
  enum batch_mode batch_mode;
  int block_given;
  VALUE results;
  int changes;
};

static VALUE batch_run_each_iter(RB_BLOCK_CALL_FUNC_ARGLIST(yield_value, vctx)) {
  struct batch_execute_each_ctx *each_ctx = (struct batch_execute_each_ctx*)vctx;
  VALUE rows = Qnil;

  sqlite3_reset(each_ctx->ctx->stmt);
  sqlite3_clear_bindings(each_ctx->ctx->stmt);
  Database_issue_query(each_ctx->ctx->db, each_ctx->ctx->sql);
  bind_all_parameters_from_object(each_ctx->ctx->stmt, yield_value);

  batch_iterate(each_ctx->ctx, each_ctx->batch_mode, &rows);
  each_ctx->changes += sqlite3_changes(each_ctx->ctx->sqlite3_db);

  if (each_ctx->batch_mode != BATCH_EXECUTE) {
    if (each_ctx->block_given)
      rb_yield(rows);
    else
      rb_ary_push(each_ctx->results, rows);
  }
  RB_GC_GUARD(rows);

  return Qnil;
}

static inline VALUE batch_run_each(query_ctx *ctx, enum batch_mode batch_mode) {
  struct batch_execute_each_ctx each_ctx = {
    .ctx          = ctx,
    .batch_mode   = batch_mode,
    .block_given  = rb_block_given_p(),
    .results      = ((batch_mode != BATCH_EXECUTE) && !rb_block_given_p() ? rb_ary_new() : Qnil),
    .changes      = 0
  };
  rb_block_call(ctx->params, ID_each, 0, 0, batch_run_each_iter, (VALUE)&each_ctx);

  if (batch_mode == BATCH_EXECUTE || each_ctx.block_given)
    return INT2FIX(each_ctx.changes);
  else
    return each_ctx.results;
}

static inline VALUE batch_run_proc(query_ctx *ctx, enum batch_mode batch_mode) {
  VALUE params = Qnil;
  int block_given = rb_block_given_p();
  VALUE results = (batch_mode != BATCH_EXECUTE) && !block_given ? rb_ary_new() : Qnil;
  VALUE rows = Qnil;
  int changes = 0;

  while (1) {
    params = rb_funcall(ctx->params, ID_call, 0);
    if (NIL_P(params)) break;

    sqlite3_reset(ctx->stmt);
    sqlite3_clear_bindings(ctx->stmt);
    Database_issue_query(ctx->db, ctx->sql);
    bind_all_parameters_from_object(ctx->stmt, params);

    batch_iterate(ctx, batch_mode, &rows);
    changes += sqlite3_changes(ctx->sqlite3_db);

    if (batch_mode != BATCH_EXECUTE) {
      if (block_given)
        rb_yield(rows);
      else
        rb_ary_push(results, rows);
    }
  }

  RB_GC_GUARD(rows);
  RB_GC_GUARD(results);
  RB_GC_GUARD(params);

  if (batch_mode == BATCH_EXECUTE || block_given)
    return INT2FIX(changes);
  else
    return results;
}

static inline VALUE batch_run(query_ctx *ctx, enum batch_mode batch_mode) {
  if (TYPE(ctx->params) == T_ARRAY)
    return batch_run_array(ctx, batch_mode);
  
  if (rb_respond_to(ctx->params, ID_each))
    return batch_run_each(ctx, batch_mode);
  
  if (rb_respond_to(ctx->params, ID_call))
    return batch_run_proc(ctx, batch_mode);
  
  rb_raise(cParameterError, "Invalid parameter source supplied to #batch_execute");
}

VALUE safe_batch_execute(query_ctx *ctx) {
  return batch_run(ctx, BATCH_EXECUTE);
}

VALUE safe_batch_query(query_ctx *ctx) {
  switch (ctx->query_mode) {
    case QUERY_HASH:
      return batch_run(ctx, BATCH_QUERY_HASH);
    case QUERY_SPLAT:
      return batch_run(ctx, BATCH_QUERY_SPLAT);
    case QUERY_ARRAY:
      return batch_run(ctx, BATCH_QUERY_ARRAY);
    default:
      rb_raise(cError, "Invalid query mode (safe_batch_query)");
  }  
}

VALUE safe_batch_query_array(query_ctx *ctx) {
  return batch_run(ctx, BATCH_QUERY_ARRAY);
}

VALUE safe_batch_query_splat(query_ctx *ctx) {
  return batch_run(ctx, BATCH_QUERY_SPLAT);
}

VALUE safe_query_columns(query_ctx *ctx) {
  return get_column_names_array(ctx->stmt, sqlite3_column_count(ctx->stmt));
}

VALUE safe_query_changes(query_ctx *ctx) {
  while (stmt_iterate(ctx));
  return INT2FIX(sqlite3_changes(ctx->sqlite3_db));
}
