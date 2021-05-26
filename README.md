## Extralite

Extralite is an extra-lightweight SQLite3 wrapper for Ruby. It provides a single
class with a minimal set of methods to interact with an SQLite3 database.

### Features

- A variety of methods for different data access patterns: row as hash, row as
  array, single single row, single column, single value.
- Iterate over records with a block, or collect records into an array.
- Parameter binding.
- Correctly execute strings with multiple semicolon-separated queries (handy for
  creating/modifying schemas).
- Get last insert rowid.
- Get number of rows changed by last query.
- Loading extensions is autmatically enabled. You can find some useful
  extensions here: https://github.com/nalgeon/sqlean.

### Usage

```ruby
require 'extralite'

# open a database
db = Extralite::Database.new('/tmp/my.db')

# get query results as array of hashes
db.query('select 1 as foo') #=> [{ :foo => 1 }]
# or:
db.query_hash('select 1 as foo') #=> [{ :foo => 1 }]
# or iterate over results
db.query('select 1 as foo') { |r| p r }
# { :foo => 1 }

# get query results as array of arrays
db.query_ary('select 1, 2, 3') #=> [[1, 2, 3]]
# or iterate over results
db.query_ary('select 1, 2, 3') { |r| p r }
# [1, 2, 3]

# get a single row as a hash
db.query_single_row("select 1 as foo") #=> { :foo => 1 }

# get single column query results as array of values
db.query_single_column('select 42') #=> [42]
# or iterate over results
db.query_single_column('select 42') { |v| p v }
# 42

# get single value from first row of results
db.query_single_value("select 'foo'") #=> "foo"

# parameter binding (works for all query_xxx methods)
db.query_hash('select ? as foo, ? as bar', 1, 2) #=> [{ :foo => 1, :bar => 2 }]

# get last insert rowid
rowid = db.last_insert_id

# get number of rows changed in last query
number_of_rows_affected = db.changes

# get db filename
db.filename #=> "/tmp/my.db"

# load an extension
db.load_extension('/path/to/extension.so')
```
