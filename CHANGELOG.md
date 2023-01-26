# 1.23 2023-01-26

- Add `Database#trace` (#21)
- Add `Database#total_changes` (#20)
- Add `Database#busy_timeout=` (#19)
- Add `Database#limit` (#16)
- Improve error handling

# 1.22 2023-01-23

- Improve documentation (#17)

# 1.21 2023-01-23

- Update bundled sqlite to version 3.40.1 (#18)

# 1.20 2023-01-21

- Fix compilation error (#15 @sitano)
- Add status methods `Extralite.runtime_status`, `Database#status`, `PreparedStatement#status` (#14 @sitano)
- Add `Database#interrupt` (#13 @sitano)
- Add `Database#backup` (#11 @sitano)
- Derive `Extralite::Error` from `StandardError` (#10 @sitano)

## 1.19 2022-12-01

- Add `Database#execute_multi`
- Add `PreparedStatement#execute_multi`

## 1.18 2022-12-01

- Fix usage with system sqlite3 lib where `load_extension` is disabled

## 1.17 2022-10-31

- Fix `Database#tables` to work on older version of sqlite (pre 3.33.0)

## 1.16 2022-10-04

- Fix `Database#pragma` to always return array of records

## 1.15 2022-10-01

- Add `Database#pragma` method
- Add `Database#tables` method

## 1.14 2022-02-28

- Introduce `extralite-bundle` gem for bundling SQLite, use system lib by
  default.

## 1.13.1 2022-02-27

- Fix compilation on TruffleRuby

## 1.13 2022-02-27

- Implement prepared statements (#7)
- Update SQLite to 3.38.0 (#6)

## 1.12 2022-02-15

- Add `Extralite.sqlite3_version` method
- Bundle sqlite3 in gem

## 1.11 2021-12-17

- Fix compilation on MacOS (#3)

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

