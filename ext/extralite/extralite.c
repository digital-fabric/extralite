#include <stdio.h>
#include "ruby.h"
#include "../sqlite3/sqlite3.h"

VALUE cError;

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
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db->sqlite3_db));
    sqlite3_close(db->sqlite3_db);
    // TODO: raise error
    return Qfalse;
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

VALUE Database_execute_hashes(VALUE self, VALUE sql) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  int done = 0;
  Database_t *db;
  GetDatabase(self, db);

  rc = sqlite3_prepare(db->sqlite3_db, RSTRING_PTR(sql), RSTRING_LEN(sql), &stmt, 0);
  if (rc) {
    sqlite3_finalize(stmt);
    rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
    return Qnil;
  }

  column_count = sqlite3_column_count(stmt);
  if (column_count > 0) {
    VALUE row;
    VALUE column_names = rb_ary_new2(column_count);
    for (int i = 0; i < column_count; i++) { 
      VALUE name = ID2SYM(rb_intern(sqlite3_column_name(stmt, i)));
      rb_ary_push(column_names, name);
    }

    row = rb_hash_new();
    while (!done) {
      rc = sqlite3_step(stmt);
      switch (rc) {
        case SQLITE_ROW:
          rb_hash_clear(row);
          for (int i = 0; i < column_count; i++) {
            VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
            rb_hash_aset(row, RARRAY_AREF(column_names, i), value);
          }
          rb_yield(row);
          continue;
        case SQLITE_DONE:
          done = 1;
          continue;
        case SQLITE_BUSY:
          rb_raise(cError, "Database is busy");
        case SQLITE_ERROR:
          rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
        default:
          rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
      }

    }

    RB_GC_GUARD(column_names);
    RB_GC_GUARD(row);
  }

  sqlite3_finalize(stmt);

  return Qnil;
}

VALUE Database_execute_arrays(VALUE self, VALUE sql) {
  int rc;
  sqlite3_stmt* stmt;
  int column_count;
  Database_t *db;
  GetDatabase(self, db);

  rc = sqlite3_prepare(db->sqlite3_db, RSTRING_PTR(sql), RSTRING_LEN(sql), &stmt, 0);
  if (rc) {
    fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db->sqlite3_db));
    sqlite3_finalize(stmt);
    // TODO: raise error
    return Qfalse;
  }

  column_count = sqlite3_column_count(stmt);
  if (column_count > 0) {
    VALUE row = rb_ary_new2(column_count);
    int done = 0;
    while (!done) {
      rc = sqlite3_step(stmt);
      switch (rc) {
        case SQLITE_ROW:
          rb_ary_clear(row);
          for (int i = 0; i < column_count; i++) {
            VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
            rb_ary_push(row, value);
          }
          rb_yield(row);
          continue;
        case SQLITE_DONE:
          done = 1;
          continue;
        case SQLITE_BUSY:
          rb_raise(cError, "Database is busy");
        case SQLITE_ERROR:
          rb_raise(cError, "%s", sqlite3_errmsg(db->sqlite3_db));
        default:
          rb_raise(cError, "Invalid return code for sqlite3_step: %d", rc);
      }
    }

    RB_GC_GUARD(row);
  }

  sqlite3_finalize(stmt);

  return Qnil;
}

void Init_Extralite() {
  VALUE mExtralite = rb_define_module("Extralite");
  VALUE cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
  rb_define_alloc_func(cDatabase, Database_allocate);

  rb_define_method(cDatabase, "initialize", Database_initialize, 1);
  rb_define_method(cDatabase, "execute_hashes", Database_execute_hashes, 1);
  rb_define_method(cDatabase, "execute_arrays", Database_execute_arrays, 1);

  cError = rb_define_class_under(mExtralite, "Error", rb_eRuntimeError);
  // rb_define_method(cDatabase, "foo", Database_foo, 0);
}