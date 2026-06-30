- [ ] Version 3.0
  - [ ] Finish work on transforms
    - [ ] More testing with prepared queries, find out if there are any rough
      edges, especially with one-to-many, many-to-many relationships
    - [ ] Docs: Transform class, README
  - [ ] Modern defaults when opening a database
    - [ ] WAL, sync, etc
    - [ ] Foreign keys
  - [ ] Run benchmarks again against latest version of sqlite3 gem

- More database methods:

  - `Database#quote`
  - `Database#cache_flush` https://sqlite.org/c3ref/db_cacheflush.html
  - `Database#release_memory` https://sqlite.org/c3ref/db_release_memory.html

- Security

  - Enable extension loading by using
    [SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION](https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension)
    in order to prevent usage of `load_extension()` SQL function.
