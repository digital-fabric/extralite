#include <stdio.h>
#include "extralite.h"

VALUE cBackup;

static size_t Backup_size(const void *ptr) {
  return sizeof(backup_t);
}

static void Backup_mark(void *ptr) {
  backup_t *obj = ptr;
  rb_gc_mark(obj->dst);
  rb_gc_mark(obj->src);
}

static void Backup_free(void *ptr) {
  backup_t *obj = ptr;
  if (obj->p) (void)sqlite3_backup_finish(obj->p);
  free(ptr);
}

static const rb_data_type_t Backup_type = {
    "Backup",
    {Backup_mark, Backup_free, Backup_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Backup_allocate(VALUE klass) {
  backup_t *obj = ALLOC(backup_t);
  obj->dst = Qnil;
  obj->src = Qnil;
  obj->p = NULL;
  return TypedData_Wrap_Struct(klass, &Backup_type, obj);
}

#define GetBackup(self, obj) \
  TypedData_Get_Struct((self), backup_t, &Backup_type, (obj))

/* call-seq: initialize(dst_db, dst_name, src_db, src_name)
 *
 * Initializes a new SQLite backup object.
 *
 * dst_db:
 *   destination Extralite::Database object.
 * dst_name:
 *   destination database name.
 * src_db:
 *   source Extralite::Database object.
 * src_name:
 *   source database name.
 *
 * This feature requires SQLite 3.6.11 or later.
 *
 *   require "extralite"
 *
 *   src = Extralite::Database.new("src")
 *   dst = Extralite::Database.new(":memory:")
 *
 *   b = Extralite::Backup.new(dst, "main", src, "main")
 *   p [b.remaining, b.pagecount] # invalid value
 *
 *   begin
 *     p b.step(1)
 *     p [b.remaining, b.pagecount]
 *   end while b.remaining > 0
 *   b.finish
 *
 *   dst = SQLite3::Database.new(":memory:")
 *   b = SQLite3::Backup.new(dst, "dst", src, "src")
 *   b.step(-1)
 *   b.finish
 *
 */
VALUE Backup_initialize(VALUE self, VALUE dst_db, VALUE dst_name, VALUE src_db, VALUE src_name) {
  backup_t *obj;
  sqlite3 *dst, *src;

  GetBackup(self, obj);

  obj->dst = dst_db;
  obj->src = src_db;

  dst = Database_sqlite3_db(obj->dst);
  src = Database_sqlite3_db(obj->src);

  if (!dst || !src) rb_raise(cError, "Database is closed");

  obj->p = sqlite3_backup_init(dst, StringValuePtr(dst_name), src, StringValuePtr(src_name));
  if (!obj->p) rb_raise(cError, "%s", sqlite3_errmsg(dst));

  return self;
}

/* call-seq:
 *   stmt.dst -> database
 *
 * Returns the database associated with the backup object.
 */
VALUE Backup_dst(VALUE self) {
  backup_t *obj;
  GetBackup(self, obj);
  return obj->dst;
}

/* call-seq:
 *   stmt.src -> database
 *
 * Returns the database associated with the backup object.
 */
VALUE Backup_src(VALUE self) {
  backup_t *obj;
  GetBackup(self, obj);
  return obj->src;
}

/* call-seq:
 *   Extralite::Backup#step(pages)
 *
 * Copy database pages up to +pages+.
 * If negative, copies everything available.
 *
 * Returns status code.
 */
static VALUE Backup_step(VALUE self, VALUE pages) {
   backup_t *obj;
  GetBackup(self, obj);
  if (!obj->p) rb_raise(cError, "Backup is closed");
  int status = sqlite3_backup_step(obj->p, NUM2INT(pages));
  return INT2NUM(status);
}

/* call-seq:
 *   Extralite::Backup#finish
 *
 * Destroy the backup object.
 */
static VALUE Backup_finish(VALUE self) {
  backup_t *obj;
  GetBackup(self, obj);
  if (!obj->p) rb_raise(cError, "Backup is closed");
  else (void)sqlite3_backup_finish(obj->p);
  obj->p = NULL;
  return Qnil;
}

/* call-seq:
 *   Extralite::Backup#remaining
 *
 * Returns the number of pages to be copied.
 *
 * The value is valid only after the first call to step().
 */
static VALUE Backup_remaining(VALUE self) {
  backup_t *obj;
  GetBackup(self, obj);
  if (!obj->p) rb_raise(cError, "Backup is closed");
  return INT2NUM(sqlite3_backup_remaining(obj->p));
}

/* call-seq:
 *   Extralite::Backup#pagecount
 *
 * Returns the total number of pages in the source database.
 *
 * The value is valid only after the first call to step().
 */
static VALUE Backup_pagecount(VALUE self) {
  backup_t *obj;
  GetBackup(self, obj);
  if (!obj->p) rb_raise(cError, "Backup is closed");
  return INT2NUM(sqlite3_backup_pagecount(obj->p));
}

void Init_ExtraliteBackup() {
  VALUE mExtralite = rb_define_module("Extralite");

  cBackup = rb_define_class_under(mExtralite, "Backup", rb_cObject);
  rb_define_alloc_func(cBackup, Backup_allocate);

  rb_define_method(cBackup, "initialize", Backup_initialize, 4);
  rb_define_method(cBackup, "dst", Backup_dst, 0);
  rb_define_method(cBackup, "src", Backup_src, 0);

  rb_define_method(cBackup, "step", Backup_step, 1);
  rb_define_method(cBackup, "finish", Backup_finish, 0);
  rb_define_method(cBackup, "remaining", Backup_remaining, 0);
  rb_define_method(cBackup, "pagecount", Backup_pagecount, 0);
}
