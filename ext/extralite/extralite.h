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
extern VALUE cPreparedStatement;

extern VALUE cError;
extern VALUE cSQLError;
extern VALUE cBusyError;

extern ID ID_KEYS;
extern ID ID_NEW;
extern ID ID_STRIP;
extern ID ID_TO_S;

typedef struct {
  sqlite3 *sqlite3_db;
} Database_t;

typedef struct {
  VALUE db;
  VALUE sql;
  sqlite3 *sqlite3_db;
  sqlite3_stmt *stmt;
} PreparedStatement_t;

typedef struct {
  VALUE self;
  sqlite3 *sqlite3_db;
  sqlite3_stmt *stmt;
  VALUE params;
} query_ctx;

VALUE safe_query_ary(query_ctx *ctx);
VALUE safe_query_hash(query_ctx *ctx);
VALUE safe_query_single_column(query_ctx *ctx);
VALUE safe_query_single_row(query_ctx *ctx);
VALUE safe_query_single_value(query_ctx *ctx);
VALUE safe_execute_multi(query_ctx *ctx);
VALUE safe_query_columns(query_ctx *ctx);

void prepare_single_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void prepare_multi_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv);
void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE obj);
int stmt_iterate(sqlite3_stmt *stmt, sqlite3 *db);
VALUE cleanup_stmt(query_ctx *ctx);

sqlite3 *Database_sqlite3_db(VALUE self);

#endif /* EXTRALITE_H */