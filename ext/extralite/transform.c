#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ruby.h"
#include "extralite.h"

VALUE cTransform;

VALUE SYM_bool;
VALUE SYM_columns;
VALUE SYM_float;
VALUE SYM_identity;
VALUE SYM_integer;
VALUE SYM_json;
VALUE SYM_name;
VALUE SYM_relation;
VALUE SYM_text;
VALUE SYM_type;

static size_t Transform_size(const void *ptr) {
  return sizeof(Transform_t);
}

static inline void transform_node_mark(struct transform_node *node) {
  rb_gc_mark_movable(node->name);
  if (node->type == TRANSFORM_T_PROC)
    rb_gc_mark_movable(node->conversion_proc);

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

  if (node->type == TRANSFORM_T_PROC)
    node->conversion_proc = rb_gc_location(node->conversion_proc);

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
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY// | RUBY_TYPED_WB_PROTECTED
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
  node->name = Qnil;
  node->conversion_proc = Qnil;
  return node;
}

struct transform_node *compile_transform_relation(VALUE spec, int *col_counter);

enum transform_node_type get_node_type(VALUE col_hash, VALUE *proc) {
  VALUE type = rb_hash_aref(col_hash, SYM_type);
  if (NIL_P(type))          return TRANSFORM_T_AUTO;
  if (type == SYM_integer)  return TRANSFORM_T_INTEGER;
  if (type == SYM_float)    return TRANSFORM_T_FLOAT;
  if (type == SYM_text)     return TRANSFORM_T_TEXT;
  if (type == SYM_bool)     return TRANSFORM_T_BOOL;
  if (type == SYM_json)     return TRANSFORM_T_JSON;
  if (type == SYM_relation) return TRANSFORM_T_RELATION;

  if (TYPE(type) == T_DATA) {
    *proc = type;
    return TRANSFORM_T_PROC;
  }

  rb_raise(cError, "Invalid column type");
}

struct transform_node *compile_transform_column(VALUE col, int *col_counter) {
  // In the PORO spec, a multi-row (array) relation is expressed as:
  //
  //     spec = {
  //       columns: {
  //         **,
  //         tags:     [{
  //           type: :relation,
  //           columns: {**}
  //         }]
  //       }
  //     }
  //
  // So we check for it here, it's also checked in compile_transform_relation
  if (TYPE(col) == T_ARRAY) goto relation_node;
  if (TYPE(col) != T_HASH)
    rb_raise(cError, "Each column must be a hash");

  VALUE proc = Qnil;
  enum transform_node_type node_type = get_node_type(col, &proc);
  if (node_type == TRANSFORM_T_RELATION) goto relation_node;

  struct transform_node *node = allocate_transform_node();
  node->type = node_type;
  node->idx = *col_counter;

  VALUE identity = rb_hash_aref(col, SYM_identity);
  if (RTEST(identity)) node->flags |= TRANSFORM_F_IDENTITY;

  if (node_type == TRANSFORM_T_PROC) node->conversion_proc = proc;

  (*col_counter)++;
  RB_GC_GUARD(proc);
  return node;

relation_node:
  return compile_transform_relation(col, col_counter);
}

struct column_iterator_ctx {
  VALUE spec;
  int *col_counter;
  struct transform_node *node;
};

int column_iterator(VALUE name, VALUE col, VALUE arg) {
  struct column_iterator_ctx *ctx = (struct column_iterator_ctx *)arg;
  struct transform_node *node = ctx->node;

  int col_idx = *(ctx->col_counter);
  struct transform_node *col_node = compile_transform_column(col, ctx->col_counter);
  col_node->flags |= TRANSFORM_F_NAME;
  col_node->name = name;

  if (col_node->flags & TRANSFORM_F_IDENTITY) {
    node->identity_node = col_node;
    node->identity_idx = col_idx;
  }

  if (node->subnodes_tail) {
    node->subnodes_tail->next = col_node;
    node->subnodes_tail = col_node;
  }
  else {
    node->subnodes_head = node->subnodes_tail = col_node;
  }

  return ST_CONTINUE;
}

struct transform_node *compile_transform_relation(VALUE spec, int *col_counter) {
  struct transform_node *node = allocate_transform_node();
  node->type = TRANSFORM_T_RELATION;

  if (TYPE(spec) == T_ARRAY) {
    node->flags |= TRANSFORM_F_ARRAY;
    spec = rb_ary_entry(spec, 0);
  }

  // val = rb_hash_aref(spec, SYM_name);
  // if (!NIL_P(val)) {
  //   node->flags |= TRANSFORM_F_NAME;
  //   node->name = val;
  // }

  VALUE val = rb_hash_aref(spec, SYM_columns);
  if (TYPE(val) != T_HASH)
    rb_raise(cError, "columns member must be a hash");

  struct column_iterator_ctx ctx = { spec, col_counter, node };
  rb_hash_foreach(val, column_iterator, (VALUE)&ctx);

  // int len = RARRAY_LEN(val);
  // for (int i = 0; i < len; i++) {
  //   VALUE col = rb_ary_entry(val, i);
  //   int col_idx = *col_counter;
  //   struct transform_node *col_node = compile_transform_column(col, col_counter);
  //   if (col_idx == identity_idx) {
  //     node->identity_node = col_node;
  //     node->identity_idx = col_idx;
  //     col_node->flags |= TRANSFORM_F_IDENTITY;
  //   }

  //   if (node->subnodes_tail) {
  //     node->subnodes_tail->next = col_node;
  //     node->subnodes_tail = col_node;
  //   }
  //   else {
  //     node->subnodes_head = node->subnodes_tail = col_node;
  //   }
  // }

  return node;
}

VALUE Transform_initialize(VALUE self, VALUE spec) {
  Transform_t *t = self_to_transform(self);
  int col_counter = 0;
  t->root = compile_transform_relation(spec, &col_counter);
  return self;
}

VALUE node_type_to_value(enum transform_node_type type) {
  switch (type) {
    case TRANSFORM_T_AUTO:    return Qnil;
    case TRANSFORM_T_INTEGER: return SYM_integer;
    case TRANSFORM_T_FLOAT:   return SYM_float;
    case TRANSFORM_T_TEXT:    return SYM_text;
    case TRANSFORM_T_BOOL:    return SYM_bool;
    case TRANSFORM_T_JSON:    return SYM_json;
    default:
      rb_raise(cError, "Invalid node type in node_type_to_value");
  }
}

VALUE transform_node_to_obj(struct transform_node *node) {
  // printf("transform_node_to_obj type: %d flags: %02x\n", node->type, node->flags);
  // if (node->flags & TRANSFORM_F_NAME) INSPECT("  name", node->name);
  VALUE hash = rb_hash_new();
  if (node->type == TRANSFORM_T_RELATION) {
    VALUE cols = rb_hash_new();

    if (node->flags & TRANSFORM_F_NAME)
      rb_hash_aset(hash, SYM_type, SYM_relation);
    rb_hash_aset(hash, SYM_columns, cols);

    struct transform_node *cur = node->subnodes_head;
    while (cur) {
      struct transform_node *next = cur->next;
      VALUE val = transform_node_to_obj(cur);
      rb_hash_aset(cols, cur->name, val);
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
  }
  else {
    switch (node->type) {
      case TRANSFORM_T_AUTO:
        break;
      case TRANSFORM_T_PROC:
        rb_hash_aset(hash, SYM_type, node->conversion_proc);
        break;
      default:
        VALUE v = node_type_to_value(node->type);
        RB_GC_GUARD(v);
        rb_hash_aset(hash, SYM_type, node_type_to_value(node->type));
    }
    if (node->flags & TRANSFORM_F_IDENTITY)
      rb_hash_aset(hash, SYM_identity, Qtrue);
    return hash;
  }
  RB_GC_GUARD(hash);
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

  SYM_bool      = ID2SYM(rb_intern("bool"));
  SYM_columns   = ID2SYM(rb_intern("columns"));
  SYM_float     = ID2SYM(rb_intern("float"));
  SYM_identity  = ID2SYM(rb_intern("identity"));
  SYM_integer   = ID2SYM(rb_intern("integer"));
  SYM_json      = ID2SYM(rb_intern("json"));
  SYM_name      = ID2SYM(rb_intern("name"));
  SYM_relation  = ID2SYM(rb_intern("relation"));
  SYM_text      = ID2SYM(rb_intern("text"));
  SYM_type      = ID2SYM(rb_intern("type"));

  rb_gc_register_mark_object(SYM_bool);
  rb_gc_register_mark_object(SYM_columns);
  rb_gc_register_mark_object(SYM_float);
  rb_gc_register_mark_object(SYM_identity);
  rb_gc_register_mark_object(SYM_integer);
  rb_gc_register_mark_object(SYM_json);
  rb_gc_register_mark_object(SYM_name);
  rb_gc_register_mark_object(SYM_relation);
  rb_gc_register_mark_object(SYM_text);
  rb_gc_register_mark_object(SYM_type);
}
