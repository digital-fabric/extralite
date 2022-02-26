#ifndef EXTRALITE_H
#define EXTRALITE_H

#include "ruby.h"
#include "ruby/thread.h"
#include <sqlite3.h>

#define SAFE(f) (VALUE (*)(VALUE))(f)

extern VALUE cError;
extern VALUE cSQLError;
extern VALUE cBusyError;

extern ID ID_KEYS;
extern ID ID_STRIP;
extern ID ID_TO_S;

typedef struct {
  sqlite3 *sqlite3_db;
} Database_t;

typedef struct {
  sqlite3 *db;
  sqlite3_stmt **stmt;
  const char *str;
  long len;
  int rc;
} multi_stmt_ctx;

typedef struct {
  VALUE self;
  sqlite3 *sqlite3_db;
  int argc;
  VALUE *argv;
  sqlite3_stmt *stmt;
} query_ctx;

VALUE safe_query_ary(query_ctx *ctx);
VALUE safe_query_hash(query_ctx *ctx);
VALUE safe_query_single_column(query_ctx *ctx);
VALUE safe_query_single_row(query_ctx *ctx);
VALUE safe_query_single_value(query_ctx *ctx);

VALUE cleanup_stmt(VALUE arg);

#endif /* EXTRALITE_H */