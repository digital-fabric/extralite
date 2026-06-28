- Transform objects:

  Ruby representation:

  ```ruby
  TRANSFORM = {
    identity_idx: 0,
    columns: [
      :id,
      :title,
      {
        name: :author,
        dentity_idx: 0,
        columns: [:id, :name]
      }
    ]
  }
  ```

  C representation:

  ```c
  struct transform_container {
    s16_t identity_idx;
    u16_t flags;
    struct transform_column *columns_head;
  }

  struct transform_column {
    struct transform_column *next;
    VALUE name;
    u16_t flags;
    struct transform_container *container;
  };
  ```

- More database methods:

  - `Database#quote`
  - `Database#cache_flush` https://sqlite.org/c3ref/db_cacheflush.html
  - `Database#release_memory` https://sqlite.org/c3ref/db_release_memory.html

- Security

  - Enable extension loading by using
    [SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION](https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension)
    in order to prevent usage of `load_extension()` SQL function.
