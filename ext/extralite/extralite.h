#ifndef EXTRALITE_H
#define EXTRALITE_H

#include "ruby.h"
#include "ruby/thread.h"
#include "ruby/encoding.h"

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
#define CALL_BLOCK(blk, val) rb_funcall(blk, ID_call, 1, val)

extern VALUE cDatabase;
extern VALUE cQuery;
extern VALUE cIterator;
extern VALUE cBlob;

extern VALUE cError;
extern VALUE cSQLError;
extern VALUE cBusyError;
extern VALUE cInterruptError;
extern VALUE cParameterError;

extern ID ID_call;
extern ID ID_keys;
extern ID ID_new;
extern ID ID_strip;

extern VALUE SYM_hash;
extern VALUE SYM_ary;
extern VALUE SYM_single_column;

typedef struct {
  sqlite3 *sqlite3_db;
  VALUE   trace_block;
  VALUE   parameter_transform_proc;
} Database_t;

typedef struct {
  VALUE         db;
  VALUE         sql;
  Database_t    *db_struct;
  sqlite3       *sqlite3_db;
  sqlite3_stmt  *stmt;
  int           eof;
  int           closed;
} Query_t;

enum iterator_mode {
  ITERATOR_HASH,
  ITERATOR_ARY,
  ITERATOR_SINGLE_COLUMN
};

typedef struct {
  VALUE               query;
  enum iterator_mode  mode;
} Iterator_t;

enum query_mode {
  QUERY_YIELD,
  QUERY_MULTI_ROW,
  QUERY_SINGLE_ROW
};

typedef struct {
  VALUE           self;
  VALUE           parameter_transform_proc;
  sqlite3         *sqlite3_db;
  sqlite3_stmt    *stmt;
  VALUE           params;
  enum query_mode mode;
  int             max_rows;
  int             eof;
} query_ctx;

#define ALL_ROWS -1
#define SINGLE_ROW -2
#define QUERY_MODE(default) (rb_block_given_p() ? QUERY_YIELD : default)
#define MULTI_ROW_P(mode) (mode == QUERY_MULTI_ROW)

extern rb_encoding *UTF8_ENCODING;

VALUE safe_execute_multi(query_ctx *ctx);
VALUE safe_query_ary(query_ctx *ctx);
VALUE safe_query_changes(query_ctx *ctx);
VALUE safe_query_columns(query_ctx *ctx);
VALUE safe_query_hash(query_ctx *ctx);
VALUE safe_query_single_column(query_ctx *ctx);
VALUE safe_query_single_row(query_ctx *ctx);
VALUE safe_query_single_value(query_ctx *ctx);

VALUE Query_each_hash(VALUE self);
VALUE Query_each_ary(VALUE self);
VALUE Query_each_single_column(VALUE self);

VALUE Query_next_hash(int argc, VALUE *argv, VALUE self);
VALUE Query_next_ary(int argc, VALUE *argv, VALUE self);
VALUE Query_next_single_column(int argc, VALUE *argv, VALUE self);

VALUE Query_to_a_hash(VALUE self);
VALUE Query_to_a_ary(VALUE self);
VALUE Query_to_a_single_column(VALUE self);

void prepare_single_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void prepare_multi_stmt(sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void bind_all_parameters(sqlite3_stmt *stmt, VALUE parameter_transform_proc, int argc, VALUE *argv);
void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE parameter_transform_proc, VALUE obj);
int stmt_iterate(query_ctx *ctx);
VALUE cleanup_stmt(query_ctx *ctx);

sqlite3 *Database_sqlite3_db(VALUE self);
Database_t *self_to_database(VALUE self);

#endif /* EXTRALITE_H */
