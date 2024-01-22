#include "ruby.h"

void Init_ExtraliteDatabase();
void Init_ExtraliteQuery();
void Init_ExtraliteIterator();
#ifdef HAVE_SQLITE3CHANGESET_NEW
void Init_ExtraliteChangeset();
#endif

void Init_extralite_ext(void) {
  rb_ext_ractor_safe(true);

  Init_ExtraliteDatabase();
  Init_ExtraliteQuery();
  Init_ExtraliteIterator();
#ifdef HAVE_SQLITE3CHANGESET_NEW
  Init_ExtraliteChangeset();
#endif
}
