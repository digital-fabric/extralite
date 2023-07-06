void Init_ExtraliteDatabase();
void Init_ExtraliteQuery();
void Init_ExtraliteIterator();

void Init_extralite_ext(void) {
  Init_ExtraliteDatabase();
  Init_ExtraliteQuery();
  Init_ExtraliteIterator();
}
