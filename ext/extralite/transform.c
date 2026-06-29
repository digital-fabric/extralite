#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ruby.h"
#include "extralite.h"

VALUE cTransform;

VALUE SYM_identity_idx;
VALUE SYM_name;
VALUE SYM_columns;

static size_t Transform_size(const void *ptr) {
  return sizeof(Transform_t);
}

static inline void transform_node_mark(struct transform_node *node) {
  if (node->flags & TRANSFORM_F_NAME)
    rb_gc_mark_movable(node->name);

  struct transform_node *cur = node->subnodes_head;
  while (cur) {
    transform_node_mark(cur);
    cur = cur->next;
  }

  if (node->next) transform_node_mark(node->next);
}

static void Transform_mark(void *ptr) {
  Transform_t *t = ptr;
  if (t->root) transform_node_mark(t->root);
}

static inline void transform_node_compact(struct transform_node *node) {
  if (node->flags & TRANSFORM_F_NAME)
    node->name = rb_gc_location(node->name);

  struct transform_node *cur = node->subnodes_head;
  while (cur) {
    transform_node_compact(cur);
    cur = cur->next;
  }

  if (node->next) transform_node_compact(node->next);
}

static void Transform_compact(void *ptr) {
  Transform_t *t = ptr;
  if (t->root) transform_node_compact(t->root);
}

static inline void transform_node_free(struct transform_node *node) {
  if (node->subnodes_head)
    transform_node_free(node->subnodes_head);
  if (node->next)
    transform_node_free(node->next);

  free(node);
}

static void Transform_free(void *ptr) {
  Transform_t *t = ptr;
  if (t->root) transform_node_free(t->root);
  free(ptr);
}

static const rb_data_type_t Transform_type = {
    "Database",
    {Transform_mark, Transform_free, Transform_size, Transform_compact},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED
};

static VALUE Transform_allocate(VALUE klass) {
  Transform_t *t = ALLOC(Transform_t);
  t->root = NULL;
  return TypedData_Wrap_Struct(klass, &Transform_type, t);
}

static inline Transform_t *self_to_transform(VALUE self) {
  Transform_t *t;
  TypedData_Get_Struct(self, Transform_t, &Transform_type, t);
  return t;
}

struct transform_node *get_transform_root(VALUE obj) {
  return self_to_transform(obj)->root;
}

inline struct transform_node *allocate_transform_node() {
  struct transform_node *node = malloc(sizeof(struct transform_node));
  memset(node, 0, sizeof(struct transform_node));
  return node;
}

struct transform_node *compile_transform_container(VALUE spec, int *col_counter);

struct transform_node *compile_transform_column(VALUE col, int *col_counter) {
  switch (TYPE(col)) {
    case T_SYMBOL: {
      struct transform_node *node = allocate_transform_node();
      node->flags |= TRANSFORM_F_NAME;
      node->idx = *col_counter;
      node->name = col;
      (*col_counter)++;
      return node;
    }
    case T_HASH:
    case T_ARRAY:
      return compile_transform_container(col, col_counter);
    default:
      rb_raise(cError, "Invalid transform spec");
  }
}

struct transform_node *compile_transform_container(VALUE spec, int *col_counter) {
  struct transform_node *node = allocate_transform_node();
  node->flags = TRANSFORM_F_CONTAINER;

  if (TYPE(spec) == T_ARRAY) {
    node->flags |= TRANSFORM_F_ARRAY;
    spec = rb_ary_entry(spec, 0);
  }

  VALUE val = rb_hash_aref(spec, SYM_identity_idx);
  int identity_idx = NIL_P(val) ? 0 : NUM2UINT(val);

  val = rb_hash_aref(spec, SYM_name);
  if (!NIL_P(val)) {
    node->flags |= TRANSFORM_F_NAME;
    node->name = val;
  }

  val = rb_hash_aref(spec, SYM_columns);
  if (NIL_P(val))
    rb_raise(cError, "Invalid transform spec");

  int len = RARRAY_LEN(val);
  for (int i = 0; i < len; i++) {
    VALUE col = rb_ary_entry(val, i);
    int col_idx = *col_counter;
    struct transform_node *col_node = compile_transform_column(col, col_counter);
    if (col_idx == identity_idx) {
      node->identity_node = col_node;
      node->identity_idx = col_idx;
      col_node->flags |= TRANSFORM_F_IDENTITY;
    }

    if (node->subnodes_tail) {
      node->subnodes_tail->next = col_node;
      node->subnodes_tail = col_node;
    }
    else {
      node->subnodes_head = node->subnodes_tail = col_node;
    }
  }

  return node;
}

VALUE Transform_initialize(VALUE self, VALUE spec) {
  Transform_t *t = self_to_transform(self);
  int col_counter = 0;
  t->root = compile_transform_container(spec, &col_counter);
  return self;
}

VALUE transform_node_to_obj(struct transform_node *node) {
  if (node->flags & TRANSFORM_F_CONTAINER) {
    VALUE hash = rb_hash_new();
    VALUE cols = rb_ary_new();

    if (node->flags & TRANSFORM_F_NAME)
      rb_hash_aset(hash, SYM_name, node->name);

    rb_hash_aset(hash, SYM_columns, cols);
    if (node->identity_node) {
      rb_hash_aset(hash, SYM_identity_idx, INT2NUM(node->identity_idx));
    }

    struct transform_node *cur = node->subnodes_head;
    while (cur) {
      struct transform_node *next = cur->next;
      VALUE val = transform_node_to_obj(cur);
      rb_ary_push(cols, val);
      RB_GC_GUARD(val);

      cur = next;
    }

    if (node->flags & TRANSFORM_F_ARRAY) {
      VALUE array = rb_ary_new();
      rb_ary_push(array, hash);
      return array;
      RB_GC_GUARD(array);
    }
    else
      return hash;
    RB_GC_GUARD(cols);
    RB_GC_GUARD(hash);
  }
  else {
    return node->name;
  }
}

VALUE Transform_to_h(VALUE self) {
  Transform_t *t = self_to_transform(self);
  return transform_node_to_obj(t->root);
}

void Init_ExtraliteTransform(void) {
  VALUE mExtralite = rb_define_module("Extralite");

  cTransform = rb_define_class_under(mExtralite, "Transform", rb_cObject);
  rb_define_alloc_func(cTransform, Transform_allocate);

  rb_define_method(cTransform, "initialize",  Transform_initialize, 1);
  rb_define_method(cTransform, "to_h",        Transform_to_h, 0);

  SYM_identity_idx  = ID2SYM(rb_intern("identity_idx"));
  SYM_name          = ID2SYM(rb_intern("name"));
  SYM_columns       = ID2SYM(rb_intern("columns"));

  rb_gc_register_mark_object(SYM_identity_idx);
  rb_gc_register_mark_object(SYM_name);
  rb_gc_register_mark_object(SYM_columns);
}
