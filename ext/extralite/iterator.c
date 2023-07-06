#include <stdio.h>
#include "extralite.h"

/*
 * Document-class: Extralite::Iterator
 *
 * This class implements an iterator used to iterate through query results.
 */

VALUE cIterator;

VALUE SYM_hash;
VALUE SYM_ary;
VALUE SYM_single_column;

static size_t Iterator_size(const void *ptr) {
  return sizeof(Iterator_t);
}

static void Iterator_mark(void *ptr) {
  Iterator_t *iterator = ptr;
  rb_gc_mark(iterator->query);
}

static const rb_data_type_t Iterator_type = {
    "Iterator",
    {Iterator_mark, free, Iterator_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Iterator_allocate(VALUE klass) {
  Iterator_t *iterator = ALLOC(Iterator_t);
  iterator->query = Qnil;
  return TypedData_Wrap_Struct(klass, &Iterator_type, iterator);
}

static inline Iterator_t *value_to_iterator(VALUE obj) {
  Iterator_t *iterator;
  TypedData_Get_Struct((obj), Iterator_t, &Iterator_type, (iterator));
  return iterator;
}

static inline enum iterator_mode symbol_to_mode(VALUE sym) {
  if (sym == SYM_hash) return ITERATOR_HASH;
  if (sym == SYM_ary) return ITERATOR_ARY;
  if (sym == SYM_single_column) return ITERATOR_SINGLE_COLUMN;

  rb_raise(cError, "Invalid iterator mode");
}

/* call-seq: initialize(query, mode)
 *
 * Initializes a new iterator
 */
VALUE Iterator_initialize(VALUE self, VALUE query, VALUE mode) {
  Iterator_t *iterator = value_to_iterator(self);

  iterator->query = query;
  iterator->mode = symbol_to_mode(mode);

  return Qnil;
}

typedef VALUE (*each_method)(VALUE);

inline each_method mode_to_each_method(enum iterator_mode mode) {
  switch (mode) {
    case ITERATOR_ARY:
      return Query_each_ary;
    case ITERATOR_SINGLE_COLUMN:
      return Query_each_single_column;
    default:
      return Query_each_hash;
  }
}

VALUE Iterator_each(VALUE self) {
  if (rb_block_given_p()) {
    Iterator_t *iterator = value_to_iterator(self);
    each_method method = mode_to_each_method(iterator->mode);
    method(iterator->query);
  }

  return self;
}

typedef VALUE (*next_method)(int, VALUE *, VALUE);

inline next_method mode_to_next_method(enum iterator_mode mode) {
  switch (mode) {
    case ITERATOR_ARY:
      return Query_next_ary;
    case ITERATOR_SINGLE_COLUMN:
      return Query_next_single_column;
    default:
      return Query_next_hash;
  }
}

VALUE Iterator_next(int argc, VALUE *argv, VALUE self) {
  Iterator_t *iterator = value_to_iterator(self);
  next_method method = mode_to_next_method(iterator->mode);
  VALUE result = method(argc, argv, iterator->query);

  return rb_block_given_p() ? self : result;
}

void Init_ExtraliteIterator(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cIterator = rb_define_class_under(mExtralite, "Iterator", rb_cObject);
  rb_define_alloc_func(cIterator, Iterator_allocate);

  rb_include_module(cIterator, rb_mEnumerable);

  rb_define_method(cIterator, "initialize", Iterator_initialize, 2);
  rb_define_method(cIterator, "each", Iterator_each, 0);
  rb_define_method(cIterator, "next", Iterator_next, -1);

  SYM_hash          = ID2SYM(rb_intern("hash"));
  SYM_ary           = ID2SYM(rb_intern("ary"));
  SYM_single_column = ID2SYM(rb_intern("single_column"));
}
