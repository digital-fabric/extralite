#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::Iterator
 *
 * This class implements an iterator used to iterate through query results.
 */

VALUE cIterator;

VALUE SYM_hash;
VALUE SYM_argv;
VALUE SYM_ary;
VALUE SYM_single_column;

static size_t Iterator_size(const void *ptr) {
  return sizeof(Iterator_t);
}

static void Iterator_mark(void *ptr) {
  Iterator_t *iterator = ptr;
  rb_gc_mark_movable(iterator->query);
}

static void Iterator_compact(void *ptr) {
  Iterator_t *iterator = ptr;
  iterator->query = rb_gc_location(iterator->query);
}

static const rb_data_type_t Iterator_type = {
    "Iterator",
    {Iterator_mark, free, Iterator_size, Iterator_compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static VALUE Iterator_allocate(VALUE klass) {
  Iterator_t *iterator = ALLOC(Iterator_t);
  iterator->query = Qnil;
  return TypedData_Wrap_Struct(klass, &Iterator_type, iterator);
}

static inline Iterator_t *self_to_iterator(VALUE obj) {
  Iterator_t *iterator;
  TypedData_Get_Struct((obj), Iterator_t, &Iterator_type, (iterator));
  return iterator;
}

/* Initializes an iterator using the given query object and iteration mode. The
 * iteration mode is one of: `:hash`, `:ary`, or `:single_column`. An iterator
 * is normally returned from one of the methods `Query#each`/`Query#each_hash`,
 * `Query#each_ary` or `Query#each_single_column` when called without a block:
 *
 *     iterator = query.each
 *     ...
 *
 * @param query [Extralite::Query] associated query
 * @param mode [Symbol] iteration mode
 * @return [void]
 */
VALUE Iterator_initialize(VALUE self, VALUE query) {
  Iterator_t *iterator = self_to_iterator(self);
  iterator->query = query;
  return Qnil;
}

/* Iterates through the associated query's result set using the iteration mode
 * set when initialized. Each row would be passed to the given block according
 * to the iteration mode, i.e. as a hash, an array, or a single value. In
 * `:single column` mode an error will be raised if the result sets contains
 * more than one columns.
 *
 * @return [Extralite::Iterator] self
 */
VALUE Iterator_each(VALUE self) {
  if (rb_block_given_p()) {
    Iterator_t *iterator = self_to_iterator(self);
    Query_each(iterator->query);
  }

  return self;
}

/* Returns the next 1 or more rows from the associated query's result set
 * according to the iteration mode, i.e. as a hash, an array or a single value.
 *
 * If no row count is given, a single row is returned. If a row count is given,
 * an array containing up to the `row_count` rows is returned. If `row_count` is
 * -1, all rows are returned. If the end of the result set has been reached,
 * `nil` is returned.
 *
 * If a block is given, rows are passed to the block and self is returned.
 *
 * @overload next()
 *   @return [Hash, Array, Object, Extralite::Iterator] next row or self if block is given
 * @overload next(row_count)
 *   @param row_count [Integer] maximum row count or -1 for all rows
 *   @return [Array, Extralite::Iterator] next rows or self if block is given
 */
VALUE Iterator_next(int argc, VALUE *argv, VALUE self) {
  Iterator_t *iterator = self_to_iterator(self);
  VALUE result = Query_next(argc, argv, iterator->query);

  return rb_block_given_p() ? self : result;
}

/* Returns all rows from the associated query's result set according to the
 * iteration mode, i.e. as a hash, an array or a single value.
 *
 * @return [Array] array of query result set rows
 */
VALUE Iterator_to_a(VALUE self) {
  Iterator_t *iterator = self_to_iterator(self);
  return Query_to_a(iterator->query);
}

/* Returns a short string representation of the iterator instance, including the
 * SQL string.
 *
 * @return [String] string representation
 */
VALUE Iterator_inspect(VALUE self) {
  VALUE cname = rb_class_name(CLASS_OF(self));

  return rb_sprintf("#<%"PRIsVALUE":%p>", cname, (void*)self);
}

void Init_ExtraliteIterator(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cIterator = rb_define_class_under(mExtralite, "Iterator", rb_cObject);
  rb_define_alloc_func(cIterator, Iterator_allocate);

  rb_include_module(cIterator, rb_mEnumerable);

  rb_define_method(cIterator, "initialize", Iterator_initialize, 1);
  rb_define_method(cIterator, "each", Iterator_each, 0);
  rb_define_method(cIterator, "inspect", Iterator_inspect, 0);
  rb_define_method(cIterator, "next", Iterator_next, -1);
  rb_define_method(cIterator, "to_a", Iterator_to_a, 0);

  SYM_hash          = ID2SYM(rb_intern("hash"));
  SYM_argv          = ID2SYM(rb_intern("argv"));
  SYM_ary           = ID2SYM(rb_intern("ary"));
  SYM_single_column = ID2SYM(rb_intern("single_column"));

  rb_gc_register_mark_object(SYM_hash);
  rb_gc_register_mark_object(SYM_argv);
  rb_gc_register_mark_object(SYM_ary);
  rb_gc_register_mark_object(SYM_single_column);
}
