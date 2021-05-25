#include <stdio.h>
#include "ruby.h"
#include "../sqlite3/sqlite3.h"

VALUE cError;
VALUE cSQLError;
VALUE cBusyError;
ID ID_STRIP;

typedef struct Database_t {
  sqlite3 *sqlite3_db;
} Database_t;

static size_t Database_size(const void *ptr) {
  return sizeof(Database_t);
}

static void Database_free(void *ptr) {
  Database_t *db = ptr;
  if (db->sqlite3_db) {
    sqlite3_close(db->sqlite3_db);
  }
  // close db
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


VALUE Database_initialize(VALUE self, VALUE path) {
  int rc;
  Database_t *db;
  GetDatabase(self, db);

  rc = sqlite3_open(StringValueCStr(path), &db->sqlite3_db);
  if (rc) {
    sqlite3_close(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  rc = sqlite3_enable_load_extension(db->sqlite3_db, 1);
  if (rc) {
    sqlite3_close(db->sqlite3_db);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
  }

  return Qnil;
}

inline VALUE get_column_value(sqlite3_stmt *stmt, int col, int type) {
  switch (type) {
    case SQLITE_NULL:
      return Qnil;
    case SQLITE_INTEGER:
      return INT2NUM(sqlite3_column_int(stmt, col));
    case SQLITE_FLOAT:
      return DBL2NUM(sqlite3_column_double(stmt, col));
    case SQLITE_TEXT:
      return rb_str_new_cstr((char *)sqlite3_column_text(stmt, col));
    case SQLITE_BLOB:
      rb_raise(cError, "BLOB reading not yet implemented");
    default:
      rb_raise(cError, "Unknown column type: %d", type);
  }

  return Qnil;
}

static inline void bind_parameter_value(sqlite3_stmt *stmt, int pos, VALUE value) {
  switch (TYPE(value)) { 
    case T_NIL:
      sqlite3_bind_null(stmt, pos);
      return;
    case T_FIXNUM:
      sqlite3_bind_int(stmt, pos, NUM2INT(value));
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
    default:
      rb_raise(cError, "Cannot bind parameter at position %d", pos);
  }
}

static inline void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv) {
  if (argc > 1) {
    for (int i = 1; i < argc; i++) {
      bind_parameter_value(stmt, i, argv[i]);
    }
  }
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

inline void prepare_multi_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql) {
  const char *rest = 0;
  const char *ptr = RSTRING_PTR(sql);
  const char *end = ptr + RSTRING_LEN(sql);
  while (1) {
    int rc = sqlite3_prepare(db, ptr, end - ptr, stmt, &rest);
    if (rc) {
      sqlite3_finalize(*stmt);
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
    }

    if (rest == end) return;
    
    // perform current query, but discard its results
    rc = sqlite3_step(*stmt);
    sqlite3_finalize(*stmt);
    switch (rc) {
    case SQLITE_BUSY:
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db));
    }
    ptr = rest;
  }
}

VALUE Database_query_hash(int argc, VALUE *argv, VALUE self) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  VALUE result = self;
  int yield_to_block = rb_block_given_p();
  VALUE row;
  VALUE column_names;
  VALUE sql;
  
  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  GetDatabase(self, db);

  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc, argv);
  column_count = sqlite3_column_count(stmt);
  column_names = get_column_names(stmt, column_count);

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();
    
step:
  rc = sqlite3_step(stmt);
  switch (rc) {
    case SQLITE_ROW:
      row = row_to_hash(stmt, column_count, column_names);
      if (yield_to_block) rb_yield(row); else rb_ary_push(result, row);
      goto step;
    case SQLITE_DONE:
      break;
    case SQLITE_BUSY:
      sqlite3_finalize(stmt);
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      sqlite3_finalize(stmt);
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db->sqlite3_db));
    default:
      sqlite3_finalize(stmt);
      rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
  }
  // TODO, use ensure to finalize statement
  sqlite3_finalize(stmt);
  RB_GC_GUARD(column_names);
  RB_GC_GUARD(row);
  RB_GC_GUARD(result);
  return result;
}

VALUE Database_query_ary(int argc, VALUE *argv, VALUE self) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  VALUE result = self;
  int yield_to_block = rb_block_given_p();
  VALUE row;
  VALUE sql;

  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;
  GetDatabase(self, db);

  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc, argv);
  column_count = sqlite3_column_count(stmt);

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();
step:
  rc = sqlite3_step(stmt);
  switch (rc) {
    case SQLITE_ROW:
      row = row_to_ary(stmt, column_count);
      if (yield_to_block) rb_yield(row); else rb_ary_push(result, row);
      goto step;
    case SQLITE_DONE:
      break;
    case SQLITE_BUSY:
      sqlite3_finalize(stmt);
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      sqlite3_finalize(stmt);
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db->sqlite3_db));
    default:
      sqlite3_finalize(stmt);
      rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
  }
  sqlite3_finalize(stmt);
  RB_GC_GUARD(row);
  RB_GC_GUARD(result);
  return result;
}

VALUE Database_query_single_row(int argc, VALUE *argv, VALUE self) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  VALUE sql;
  VALUE row = Qnil;
  VALUE column_names;

  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  GetDatabase(self, db);

  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc, argv);
  column_count = sqlite3_column_count(stmt);
  column_names = get_column_names(stmt, column_count);

  rc = sqlite3_step(stmt);
  switch (rc) {
    case SQLITE_ROW:
      row = row_to_hash(stmt, column_count, column_names);
      break;
    case SQLITE_DONE:
      break;
    case SQLITE_BUSY:
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db->sqlite3_db));
    default:
      rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
  }

  sqlite3_finalize(stmt);
  RB_GC_GUARD(row);
  RB_GC_GUARD(column_names);
  return row;
}

VALUE Database_query_single_column(int argc, VALUE *argv, VALUE self) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  VALUE result = self;
  int yield_to_block = rb_block_given_p();
  VALUE sql;
  VALUE value;

  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  GetDatabase(self, db);

  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc, argv);
  column_count = sqlite3_column_count(stmt);
  if (column_count != 1)
    rb_raise(cError, "Expected query result to have 1 column");

  // block not given, so prepare the array of records to be returned
  if (!yield_to_block) result = rb_ary_new();
step:
  rc = sqlite3_step(stmt);
  switch (rc) {
    case SQLITE_ROW:
      value = get_column_value(stmt, 0, sqlite3_column_type(stmt, 0));
      if (yield_to_block) rb_yield(value); else rb_ary_push(result, value);
      goto step;
    case SQLITE_DONE:
      break;
    case SQLITE_BUSY:
      sqlite3_finalize(stmt);
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      sqlite3_finalize(stmt);
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db->sqlite3_db));
    default:
      sqlite3_finalize(stmt);
      rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
  }

  sqlite3_finalize(stmt);
  RB_GC_GUARD(value);
  RB_GC_GUARD(result);
  return result;
}

VALUE Database_query_single_value(int argc, VALUE *argv, VALUE self) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  VALUE sql;
  VALUE value = Qnil;

  rb_check_arity(argc, 1, UNLIMITED_ARGUMENTS);
  sql = rb_funcall(argv[0], ID_STRIP, 0);
  if (RSTRING_LEN(sql) == 0) return Qnil;

  GetDatabase(self, db);

  prepare_multi_stmt(db->sqlite3_db, &stmt, sql);
  bind_all_parameters(stmt, argc, argv);
  column_count = sqlite3_column_count(stmt);
  if (column_count != 1)
    rb_raise(cError, "Expected query result to have 1 column");

  rc = sqlite3_step(stmt);
  switch (rc) {
    case SQLITE_ROW:
      value = get_column_value(stmt, 0, sqlite3_column_type(stmt, 0));
      break;
    case SQLITE_DONE:
      break;
    case SQLITE_BUSY:
      rb_raise(cBusyError, "Database is busy");
    case SQLITE_ERROR:
      rb_raise(cSQLError, "%s", sqlite3_errmsg(db->sqlite3_db));
    default:
      rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
  }

  sqlite3_finalize(stmt);
  RB_GC_GUARD(value);
  return value;
}

VALUE Database_last_insert_rowid(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);

  return INT2NUM(sqlite3_last_insert_rowid(db->sqlite3_db));
}

VALUE Database_changes(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);

  return INT2NUM(sqlite3_changes(db->sqlite3_db));
}

VALUE Database_filename(int argc, VALUE *argv, VALUE self) {
  const char *db_name;
  const char *filename;
  Database_t *db;
  GetDatabase(self, db);

  rb_check_arity(argc, 0, 1);
  db_name = (argc == 1) ? StringValueCStr(argv[0]) : "main";
  filename = sqlite3_db_filename(db->sqlite3_db, db_name);
  return filename ? rb_str_new_cstr(filename) : Qnil;
}

VALUE Database_transaction_active_p(VALUE self) {
  Database_t *db;
  GetDatabase(self, db);

  return sqlite3_get_autocommit(db->sqlite3_db) ? Qfalse : Qtrue;
}

VALUE Database_load_extension(VALUE self, VALUE path) {
  Database_t *db;
  GetDatabase(self, db);
  char *err_msg;

  int rc = sqlite3_load_extension(db->sqlite3_db, RSTRING_PTR(path), 0, &err_msg);
  if (rc != SQLITE_OK) {
    VALUE error = rb_exc_new2(cError, err_msg);
    sqlite3_free(err_msg);
    rb_exc_raise(error);
  }

  return self;
}

void Init_Extralite() {
  VALUE mExtralite = rb_define_module("Extralite");
  VALUE cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
  rb_define_alloc_func(cDatabase, Database_allocate);

  rb_define_method(cDatabase, "initialize", Database_initialize, 1);
  
  rb_define_method(cDatabase, "query", Database_query_hash, -1);
  rb_define_method(cDatabase, "query_hash", Database_query_hash, -1);
  rb_define_method(cDatabase, "query_ary", Database_query_ary, -1);
  rb_define_method(cDatabase, "query_single_row", Database_query_single_row, -1);
  rb_define_method(cDatabase, "query_single_column", Database_query_single_column, -1);
  rb_define_method(cDatabase, "query_single_value", Database_query_single_value, -1);
  
  rb_define_method(cDatabase, "last_insert_rowid", Database_last_insert_rowid, 0);
  rb_define_method(cDatabase, "changes", Database_changes, 0);
  rb_define_method(cDatabase, "filename", Database_filename, -1);
  rb_define_method(cDatabase, "transaction_active?", Database_transaction_active_p, 0);
  rb_define_method(cDatabase, "load_extension", Database_load_extension, 1);

  cError = rb_define_class_under(mExtralite, "Error", rb_eRuntimeError);
  cSQLError = rb_define_class_under(mExtralite, "SQLError", cError);
  cBusyError = rb_define_class_under(mExtralite, "BusyError", cError);

  ID_STRIP = rb_intern("strip");
}
