- Transform objects:

  - Transform DSL:

  ```ruby
  Extralite::Transform.new do
    {
      id:       integer.identity,
      title:    text,
      content:  text,
      tags:     [{
        id:     integer.identity,
        name:   text
      }]
    }
  end
  ```

- More database methods:

  - `Database#quote`
  - `Database#cache_flush` https://sqlite.org/c3ref/db_cacheflush.html
  - `Database#release_memory` https://sqlite.org/c3ref/db_release_memory.html

- Security

  - Enable extension loading by using
    [SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION](https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension)
    in order to prevent usage of `load_extension()` SQL function.
