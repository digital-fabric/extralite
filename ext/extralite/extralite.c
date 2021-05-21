#include <stdio.h>
#include "ruby.h"
#include "../sqlite3/sqlite3.h"

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
  fprintf(stderr, "get_column_value col=%d type=%d\n", col, type);
  switch (type) {
    case SQLITE_INTEGER:
      return INT2NUM(sqlite3_column_int(stmt, col));
    case SQLITE_FLOAT:
      return DBL2NUM(sqlite3_column_double(stmt, col));
    case SQLITE_TEXT:
      return rb_str_new_cstr((char *)sqlite3_column_text(stmt, col));
    case SQLITE_BLOB:
      // TODO: implement
      return Qfalse;
    case SQLITE_NULL:
      return Qnil;
    default:
      // TODO: raise error
      return Qfalse;
  }
}

VALUE Database_execute(VALUE self, VALUE sql) {
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
    VALUE hash;
    VALUE column_names = rb_ary_new2(column_count);
    for (int i = 0; i < column_count; i++) { 
      VALUE name = ID2SYM(rb_intern(sqlite3_column_name(stmt, i)));
      rb_ary_push(column_names, name);
    }

    hash = rb_hash_new();
    while (1) {
      rc = sqlite3_step(stmt);
      fprintf(stderr, "step rc=%d\n", rc);
      if (rc == SQLITE_ROW) {
        rb_hash_clear(hash);
        for (int i = 0; i < column_count; i++) {
          VALUE value = get_column_value(stmt, i, sqlite3_column_type(stmt, i));
          rb_hash_aset(hash, RARRAY_AREF(column_names, i), value);
        }
        rb_yield(hash);
      }
      break;
      switch (rc) {
        case SQLITE_DONE:
          break;
        case SQLITE_BUSY:
          break;
        case SQLITE_ERROR:
          fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db->sqlite3_db));
          break;
        default:
          fprintf(stderr, "rc = %d\n", rc);
          break;
      }

    }

    RB_GC_GUARD(column_names);
    RB_GC_GUARD(hash);
  }

  sqlite3_finalize(stmt);

  return Qnil;
}

void Init_Extralite() {
  VALUE mExtralite = rb_define_module("Extralite");
  VALUE cDatabase = rb_define_class_under(mExtralite, "Database", rb_cObject);
  rb_define_alloc_func(cDatabase, Database_allocate);

  rb_define_method(cDatabase, "initialize", Database_initialize, 1);
  rb_define_method(cDatabase, "execute", Database_execute, 1);
  // rb_define_method(cDatabase, "foo", Database_foo, 0);
}