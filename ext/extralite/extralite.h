#ifndef EXTRALITE_H
#define EXTRALITE_H

#include "ruby.h"
#include "ruby/thread.h"

#ifdef EXTRALITE_NO_BUNDLE
#include <sqlite3.h>
#else
#include "../sqlite3/sqlite3.h"
#endif

// debug utility
#define INSPECT(str, obj) { \
  printf(str); \
  VALUE s = rb_funcall(obj, rb_intern("inspect"), 0); \
  printf(": %s\n", StringValueCStr(s)); \
}

#define SAFE(f) (VALUE (*)(VALUE))(f)

extern VALUE cDatabase;
extern VALUE cQuery;

extern VALUE cError;
extern VALUE cSQLError;
extern VALUE cBusyError;
extern VALUE cInterruptError;

extern ID ID_call;
extern ID ID_keys;
extern ID ID_new;
extern ID ID_strip;
extern ID ID_to_s;

typedef struct {
  sqlite3 *sqlite3_db;
  VALUE   trace_block;
} Database_t;

typedef struct {
  VALUE         db;
  VALUE         sql;
  Database_t    *db_struct;
  sqlite3       *sqlite3_db;
  sqlite3_stmt  *stmt;
  int           closed;
} Query_t;

typedef struct {
  VALUE         self;
  sqlite3       *sqlite3_db;
  sqlite3_stmt  *stmt;
  VALUE         params;
} query_ctx;

typedef struct {
  VALUE           dst;
  VALUE           src;
  sqlite3_backup  *p;
} backup_t;

#define TUPLE_MAX_EMBEDDED_VALUES 20

VALUE safe_execute_multi(query_ctx *ctx);
VALUE safe_query_ary(query_ctx *ctx);
VALUE safe_query_columns(query_ctx *ctx);
VALUE safe_query_hash(query_ctx *ctx);
VALUE safe_query_single_column(query_ctx *ctx);
VALUE safe_query_single_row(query_ctx *ctx);
VALUE safe_query_single_value(query_ctx *ctx);

void prepare_single_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void prepare_multi_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv);
void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE obj);
int stmt_iterate(sqlite3_stmt *stmt, sqlite3 *db);
VALUE cleanup_stmt(query_ctx *ctx);

sqlite3 *Database_sqlite3_db(VALUE self);
Database_t *Database_struct(VALUE self);

#endif /* EXTRALITE_H */