void Init_ExtraliteDatabase();
void Init_ExtralitePreparedStatement();
void Init_ExtraliteBackup();

void Init_extralite_ext(void) {
  Init_ExtraliteDatabase();
  Init_ExtralitePreparedStatement();
  Init_ExtraliteBackup();
}
