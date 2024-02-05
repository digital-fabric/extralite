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
#define CALLER() rb_funcall(rb_mKernel, rb_intern("caller"), 0)
#define TRACE_CALLER() INSPECT("caller: ", CALLER())

#define SAFE(f) (VALUE (*)(VALUE))(f)

extern VALUE cDatabase;
extern VALUE cQuery;
extern VALUE cIterator;
extern VALUE cChangeset;
extern VALUE cBlob;

extern VALUE cError;
extern VALUE cSQLError;
extern VALUE cBusyError;
extern VALUE cInterruptError;
extern VALUE cParameterError;

extern ID ID_call;
extern ID ID_each;
extern ID ID_keys;
extern ID ID_new;
extern ID ID_strip;
extern ID ID_to_s;
extern ID ID_track;

extern VALUE SYM_argv;
extern VALUE SYM_ary;
extern VALUE SYM_hash;

enum progress_handler_mode {
  PROGRESS_NONE,
  PROGRESS_NORMAL,
  PROGRESS_ONCE,
  PROGRESS_AT_LEAST_ONCE,
};

struct progress_handler {
  enum progress_handler_mode  mode;
  VALUE                       proc;
  int                         period;
  int                         tick;
  int                         tick_count;
  int                         call_count;
};

typedef struct {
  sqlite3                 *sqlite3_db;
  VALUE                   trace_proc;
  int                     gvl_release_threshold;
  struct progress_handler progress_handler;
} Database_t;

enum query_mode {
  QUERY_HASH,
  QUERY_ARGV,
  QUERY_ARY
};

typedef struct {
  VALUE               db;
  VALUE               sql;
  VALUE               transform_proc;
  Database_t          *db_struct;
  sqlite3             *sqlite3_db;
  sqlite3_stmt        *stmt;
  int                 eof;
  int                 closed;
  enum query_mode     query_mode;
} Query_t;

enum iterator_mode {
  ITERATOR_HASH,
  ITERATOR_ARGV,
  ITERATOR_ARY,
  ITERATOR_SINGLE_COLUMN
};

typedef struct {
  VALUE               query;
} Iterator_t;

#ifdef EXTRALITE_ENABLE_CHANGESET
typedef struct {
  int             changeset_len;
  void            *changeset_ptr;
} Changeset_t;
#endif

enum row_mode {
  ROW_YIELD,
  ROW_MULTI,
  ROW_SINGLE
};

typedef struct {
  VALUE               self;
  VALUE               sql;
  VALUE               params;
  VALUE               transform_proc;

  Database_t          *db;
  sqlite3             *sqlite3_db;
  sqlite3_stmt        *stmt;

  int                 gvl_release_threshold;
  enum query_mode     query_mode;
  enum row_mode       row_mode;
  int                 max_rows;

  int                 eof;
  int                 step_count;
} query_ctx;

enum gvl_mode {
  GVL_RELEASE,
  GVL_HOLD
};

#define ALL_ROWS -1
#define SINGLE_ROW -2
#define ROW_YIELD_OR_MODE(default) (rb_block_given_p() ? ROW_YIELD : default)
#define ROW_MULTI_P(mode) (mode == ROW_MULTI)
#define QUERY_CTX(self, sql, db, stmt, params, transform_proc, query_mode, row_mode, max_rows) { \
  self, \
  sql, \
  params, \
  transform_proc, \
  db, \
  db->sqlite3_db, \
  stmt, \
  db->gvl_release_threshold, \
  query_mode, \
  row_mode, \
  max_rows, \
  0, \
  0 \
}

#define DEFAULT_GVL_RELEASE_THRESHOLD 1000

extern rb_encoding *UTF8_ENCODING;

typedef VALUE (*safe_query_impl)(query_ctx *);

VALUE safe_batch_execute(query_ctx *ctx);
VALUE safe_batch_query(query_ctx *ctx);
VALUE safe_batch_query_argv(query_ctx *ctx);
VALUE safe_batch_query_ary(query_ctx *ctx);
VALUE safe_query_argv(query_ctx *ctx);
VALUE safe_query_ary(query_ctx *ctx);
VALUE safe_query_changes(query_ctx *ctx);
VALUE safe_query_columns(query_ctx *ctx);
VALUE safe_query_hash(query_ctx *ctx);
VALUE safe_query_single_row_hash(query_ctx *ctx);
VALUE safe_query_single_row_argv(query_ctx *ctx);
VALUE safe_query_single_row_ary(query_ctx *ctx);

VALUE Query_each(VALUE self);
VALUE Query_next(int argc, VALUE *argv, VALUE self);
VALUE Query_to_a(VALUE self);

void prepare_single_stmt(enum gvl_mode mode, sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void prepare_multi_stmt(enum gvl_mode mode, sqlite3 *db, sqlite3_stmt **stmt, VALUE sql);
void bind_all_parameters(sqlite3_stmt *stmt, int argc, VALUE *argv);
void bind_all_parameters_from_object(sqlite3_stmt *stmt, VALUE obj);
int stmt_iterate(query_ctx *ctx);
VALUE cleanup_stmt(query_ctx *ctx);

void Database_issue_query(Database_t *db, VALUE sql);
sqlite3 *Database_sqlite3_db(VALUE self);
enum gvl_mode Database_prepare_gvl_mode(Database_t *db);
Database_t *self_to_database(VALUE self);

void *gvl_call(enum gvl_mode mode, void *(*fn)(void *), void *data);

#endif /* EXTRALITE_H */
