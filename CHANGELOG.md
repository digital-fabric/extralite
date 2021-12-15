## 1.10 2021-12-15

- Fix mutliple parameter binding with hash

## 1.9 2021-12-15

- Add support for reading BLOBs

## 1.8.2 2021-12-15

- Add documentation

## 1.7 2021-12-13

- Add extralite Sequel adapter
- Add support for binding hash parameters

## 1.6 2021-12-13

- Release GVL while fetching rows

## 1.5 2021-12-13

- Release GVL while preparing statements
- Use `sqlite3_prepare_v2` instead of deprecated `sqlite_prepare`

## 1.4 2021-08-25

- Fix possible segfault in cleanup_stmt

## 1.3 2021-08-17

- Pin error classes (for better compatibility with `GC.compact`)

## 1.2 2021-06-06

- Add support for big integers

## 1.1 2021-06-02

- Add `#close`, `#closed?` methods

## 1.0 2021-05-27

- Refactor C code
- Use `rb_ensure` to finalize stmt
- Remove bundled `sqlite3.h`, use system-wide header file instead

## 0.6 2021-05-25

- Add more specific errors: `SQLError`, `BusyError`

## 0.5 2021-05-25

- Implement `Database#query_single_row`

## 0.4 2021-05-24

- Add support for loading extensions

## 0.3 2021-05-24

- Add support for running multiple statements

## 0.2 2021-05-23

- Implement `Database#transaction_active?`
- Add tests

## 0.1 2021-05-21

- First release

