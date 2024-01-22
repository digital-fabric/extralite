#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::Changeset
 *
 * This class implements a Changeset for tracking changes to the database.
 */

VALUE cChangeset;

VALUE SYM_delete;
VALUE SYM_insert;
VALUE SYM_update;

static size_t Changeset_size(const void *ptr) {
  return sizeof(Changeset_t);
}

static void Changeset_mark(void *ptr) {
  // Changeset_t *changeset = ptr;
  // rb_gc_mark_movable(changeset->db);
}

static void Changeset_free(void *ptr) {
  Changeset_t *changeset = ptr;
  if (changeset->session)
    sqlite3session_delete(changeset->session);
  if (changeset->changeset_ptr)
    sqlite3_free(changeset->changeset_ptr);
  free(ptr);
}

static const rb_data_type_t Changeset_type = {
    "Changeset",
    {Changeset_mark, Changeset_free, Changeset_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Changeset_allocate(VALUE klass) {
  Changeset_t *changeset = ALLOC(Changeset_t);
  changeset->session = NULL;
  changeset->changeset_len = 0;
  changeset->changeset_ptr = NULL;
  return TypedData_Wrap_Struct(klass, &Changeset_type, changeset);
}

static inline Changeset_t *self_to_changeset(VALUE obj) {
  Changeset_t *changeset;
  TypedData_Get_Struct((obj), Changeset_t, &Changeset_type, (changeset));
  return changeset;
}

VALUE Changeset_initialize(VALUE self) {
  Changeset_t *changeset = self_to_changeset(self);

  changeset->session = NULL;
  changeset->changeset_len = 0;
  changeset->changeset_ptr = NULL;

  return Qnil;
}

static inline VALUE tbl_str(VALUE tbl) {
  switch (TYPE(tbl)) {
    case T_NIL:
    case T_STRING:
      return tbl;
    default:
      return rb_funcall(tbl, ID_to_s, 0);
  }
}

void Changeset_track_attach(sqlite3 *db, sqlite3_session *session, VALUE tables) {
  long len = RARRAY_LEN(tables);
  VALUE name = Qnil;
  for (long i = 0; i < len; i++) {
    name = tbl_str(RARRAY_AREF(tables, i));
    int rc = sqlite3session_attach(session, NIL_P(name) ? NULL : StringValueCStr(name));
    if (rc != SQLITE_OK)
      rb_raise(cError, "Error while attaching session tables: %s", sqlite3_errstr(rc));
  }
  RB_GC_GUARD(name);
}

VALUE Changeset_track(VALUE self, VALUE db, VALUE tables) {
  Changeset_t *changeset = self_to_changeset(self);
  Database_t *db_struct = self_to_database(db);
  sqlite3 *sqlite3_db = db_struct->sqlite3_db;

  if (changeset->session) {
    sqlite3session_delete(changeset->session);
    changeset->session = NULL;
  }
  if (changeset->changeset_ptr) {
    sqlite3_free(changeset->changeset_ptr);
    changeset->changeset_len = 0;
    changeset->changeset_ptr = NULL;
  }

  int rc = sqlite3session_create(sqlite3_db, "main", &changeset->session);
  if (rc != SQLITE_OK)
    rb_raise(cError, "Error while creating session: %s", sqlite3_errstr(rc));

  if (!NIL_P(tables))
    Changeset_track_attach(sqlite3_db, changeset->session, tables);
  else {
    rc = sqlite3session_attach(changeset->session, NULL);
    if (rc != SQLITE_OK)
      rb_raise(cError, "Error while attaching all tables: %s", sqlite3_errstr(rc));
  }

  rb_yield(db);

  return self;
}

struct each_ctx {
  sqlite3_changeset_iter *iter;
};

static inline VALUE op_symbol(int op) {
  switch (op) {
    case SQLITE_DELETE:
      return SYM_delete;
    case SQLITE_INSERT:
      return SYM_insert;
    case SQLITE_UPDATE:
      return SYM_update;
    default:
      rb_raise(cError, "Invalid changeset op code %d", op);
  }
}

VALUE convert_value(sqlite3_value *value) {
  if (!value) return Qnil;

  int type = sqlite3_value_type(value);
  switch (type) {
    case SQLITE_INTEGER:
      return LL2NUM(sqlite3_value_int64(value));
    case SQLITE_FLOAT:
      return DBL2NUM(sqlite3_value_double(value));
    case SQLITE_NULL:
      return Qnil;
    case SQLITE_BLOB:
      {
        int len = sqlite3_value_bytes(value);
        void *blob = sqlite3_value_blob(value);
        return rb_str_new(blob, len);
      }
    case SQLITE_TEXT:
      {
        int len = sqlite3_value_bytes(value);
        void *text = sqlite3_value_text(value);
        return rb_enc_str_new(text, len, UTF8_ENCODING);
      }
    default:
      rb_raise(cError, "Invalid value type: %d\n", type);
  }
}

VALUE changeset_iter_info(sqlite3_changeset_iter *iter) {
  VALUE op = Qnil;
  VALUE tbl = Qnil;
  VALUE old_values = Qnil;
  VALUE new_values = Qnil;
  VALUE converted = Qnil;
  VALUE row = rb_ary_new2(4);
  
  const char *tbl_name;
  int column_count;
  int op_int;

  int rc = sqlite3changeset_op(iter, &tbl_name, &column_count, &op_int, NULL);
  if (rc != SQLITE_OK)
    rb_raise(cError, "Error while iterating (sqlite3changeset_op): %s", sqlite3_errstr(rc));

  op = op_symbol(op_int);
  tbl = rb_str_new_cstr(tbl_name);

  if (op_int == SQLITE_UPDATE || op_int == SQLITE_DELETE) {
    sqlite3_value *value = NULL;
    old_values = rb_ary_new2(column_count);
    for (int i = 0; i < column_count; i++) {
      rc = sqlite3changeset_old(iter, i, &value);
      if (rc != SQLITE_OK)
        rb_raise(cError, "Error while iterating (sqlite3changeset_old): %s", sqlite3_errstr(rc));
      converted = convert_value(value);
      rb_ary_push(old_values, converted);
    }
  }

  if (op_int == SQLITE_UPDATE || op_int == SQLITE_INSERT) {
    sqlite3_value *value = NULL;
    new_values = rb_ary_new2(column_count);
    for (int i = 0; i < column_count; i++) {
      rc = sqlite3changeset_new(iter, i, &value);
      if (rc != SQLITE_OK)
        rb_raise(cError, "Error while iterating (sqlite3changeset_new): %s", sqlite3_errstr(rc));
      converted = convert_value(value);
      rb_ary_push(new_values, converted);
    }
  }

  rb_ary_push(row, op);
  rb_ary_push(row, tbl);
  rb_ary_push(row, old_values);
  rb_ary_push(row, new_values);

  RB_GC_GUARD(op);
  RB_GC_GUARD(tbl);
  RB_GC_GUARD(old_values);
  RB_GC_GUARD(new_values);
  RB_GC_GUARD(converted);

  return row;
}

VALUE safe_each(struct each_ctx *ctx) {
  VALUE row = Qnil;
  while (sqlite3changeset_next(ctx->iter) == SQLITE_ROW) {
    row = changeset_iter_info(ctx->iter);
    rb_yield_splat(row);
  }

  RB_GC_GUARD(row);
  return Qnil;
}

VALUE safe_to_a(struct each_ctx *ctx) {
  VALUE row = Qnil;
  VALUE array = rb_ary_new();
  while (sqlite3changeset_next(ctx->iter) == SQLITE_ROW) {
    row = changeset_iter_info(ctx->iter);
    rb_ary_push(array, row);
  }

  RB_GC_GUARD(row);
  RB_GC_GUARD(array);
  return array;
}

VALUE cleanup_iter(struct each_ctx *ctx) {
  int rc = sqlite3changeset_finalize(ctx->iter);
  if (rc != SQLITE_OK)
    rb_raise(cError, "Error while finalizing changeset iterator: %s", sqlite3_errstr(rc));

  return Qnil;
}

void verify_changeset(Changeset_t *changeset) {
  if (!changeset->changeset_ptr) {
    if (!changeset->session)
      rb_raise(cError, "Changeset not available");
    
    int rc = sqlite3session_changeset(
      changeset->session,
      &changeset->changeset_len,
      &changeset->changeset_ptr
    );
    if (rc != SQLITE_OK)
      rb_raise(cError, "Error while collecting changeset from session: %s", sqlite3_errstr(rc));
  }
}

VALUE Changeset_each(VALUE self) {
  Changeset_t *changeset = self_to_changeset(self);
  verify_changeset(changeset);

  struct each_ctx ctx = { .iter = NULL };
  int rc = sqlite3changeset_start(&ctx.iter, changeset->changeset_len, changeset->changeset_ptr);
  if (rc!=SQLITE_OK)
    rb_raise(cError, "Error while starting iterator: %s", sqlite3_errstr(rc));

  rb_ensure(SAFE(safe_each), (VALUE)&ctx, SAFE(cleanup_iter), (VALUE)&ctx);
  return self;
}

VALUE Changeset_to_a(VALUE self) {
  Changeset_t *changeset = self_to_changeset(self);
  verify_changeset(changeset);

  struct each_ctx ctx = { .iter = NULL };
  int rc = sqlite3changeset_start(&ctx.iter, changeset->changeset_len, changeset->changeset_ptr);
  if (rc!=SQLITE_OK)
    rb_raise(cError, "Error while starting iterator: %s", sqlite3_errstr(rc));

  return rb_ensure(SAFE(safe_to_a), (VALUE)&ctx, SAFE(cleanup_iter), (VALUE)&ctx);
}

// copied from: https://sqlite.org/sessionintro.html
static int xConflict(void *pCtx, int eConflict, sqlite3_changeset_iter *pIter){
  int ret = (long)pCtx;
  return ret;
}

VALUE Changeset_apply(VALUE self, VALUE db) {
  Changeset_t *changeset = self_to_changeset(self);
  verify_changeset(changeset);

  Database_t *db_struct = self_to_database(db);
  sqlite3 *sqlite3_db = db_struct->sqlite3_db;

  int rc = sqlite3changeset_apply(
    sqlite3_db,
    changeset->changeset_len,
    changeset->changeset_ptr,
    NULL,
    xConflict,
    (void*)1
  );
  if (rc != SQLITE_OK)
    rb_raise(cError, "Error while applying changeset: %s", sqlite3_errstr(rc));

  return self;
}

VALUE Changeset_invert(VALUE self) {
  Changeset_t *changeset = self_to_changeset(self);
  verify_changeset(changeset);

  VALUE inverted = rb_funcall(cChangeset, ID_new, 0);
  Changeset_t *inverted_changeset = self_to_changeset(inverted);

  int rc = sqlite3changeset_invert(
    changeset->changeset_len, changeset->changeset_ptr,
    &inverted_changeset->changeset_len, &inverted_changeset->changeset_ptr
  );
  if (rc != SQLITE_OK)
    rb_raise(cError, "Error while inverting changeset: %s", sqlite3_errstr(rc));

  RB_GC_GUARD(inverted);
  return inverted;
}

void Init_ExtraliteChangeset(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cChangeset = rb_define_class_under(mExtralite, "Changeset", rb_cObject);
  rb_define_alloc_func(cChangeset, Changeset_allocate);

  rb_define_method(cChangeset, "initialize", Changeset_initialize, 0);

  rb_define_method(cChangeset, "apply", Changeset_apply, 1);
  rb_define_method(cChangeset, "each", Changeset_each, 0);
  rb_define_method(cChangeset, "invert", Changeset_invert, 0);
  rb_define_method(cChangeset, "to_a", Changeset_to_a, 0);
  rb_define_method(cChangeset, "track", Changeset_track, 2);

  SYM_delete = ID2SYM(rb_intern("delete"));
  SYM_insert = ID2SYM(rb_intern("insert"));
  SYM_update = ID2SYM(rb_intern("update"));

  rb_gc_register_mark_object(SYM_delete);
  rb_gc_register_mark_object(SYM_insert);
  rb_gc_register_mark_object(SYM_update);
}
