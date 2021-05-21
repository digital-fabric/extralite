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

typedef struct ExecuteContext_t {
  VALUE block;
} ExecuteContext_t;

static int callback_block_hash(void *ctx, int argc, char **argv, char **azColName) {
  // ExecuteContext_t *execute_context = ctx;
  
  VALUE hash = rb_hash_new();
  for (int i=0; i < argc; i++) {
    rb_hash_aset(hash, rb_str_new_cstr(azColName[i]), rb_str_new_cstr(argv[i]));
    // printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  // printf("\n");
  rb_yield(hash);
  RB_GC_GUARD(hash);
  return 0;
}

VALUE Database_execute(VALUE self, VALUE sql) {
  int rc;
  char *zErrMsg = 0;
  ExecuteContext_t ctx;
  Database_t *db;
  GetDatabase(self, db);

  ctx.block = rb_block_given_p() ? rb_block_proc() : Qnil;
  rc = sqlite3_exec(db->sqlite3_db, StringValueCStr(sql), callback_block_hash, &ctx, &zErrMsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
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