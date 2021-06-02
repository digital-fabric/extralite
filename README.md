# Extralite - a Ruby gem for working with SQLite3 databases

[![Gem Version](https://badge.fury.io/rb/extralite.svg)](http://rubygems.org/gems/extralite)
[![Modulation Test](https://github.com/digital-fabric/extralite/workflows/Tests/badge.svg)](https://github.com/digital-fabric/extralite/actions?query=workflow%3ATests)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/digital-fabric/extralite/blob/master/LICENSE)

## What is Extralite?

Extralite is an extra-lightweight (less than 400 lines of C-code) SQLite3 wrapper for
Ruby. It provides a single class with a minimal set of methods to interact with
an SQLite3 database.

## Features

- A variety of methods for different data access patterns: row as hash, row as
  array, single single row, single column, single value.
- Iterate over records with a block, or collect records into an array.
- Parameter binding.
- Correctly execute strings with multiple semicolon-separated queries (handy for
  creating/modifying schemas).
- Get last insert rowid.
- Get number of rows changed by last query.
- Load extensions (loading of extensions is autmatically enabled. You can find
  some useful extensions here: https://github.com/nalgeon/sqlean.)

## Usage

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

# close database
db.close
db.closed? #=> true
```

## Why not just use the sqlite3 gem?

The sqlite3-ruby gem is a popular, solid, well-maintained project, used by
thousands of developers. I've been doing a lot of work with SQLite3 lately, and
wanted to have a simpler API that gives me query results in a variety of ways.
Thus extralite was born.

## What about concurrency?

Extralite currently does not release the GVL. This means that even if queries
are executed on a separate thread, no other Ruby threads will be scheduled while
SQLite3 is busy fetching the next record.

In the future Extralite might be changed to release the GVL each time
`sqlite3_step` is called.

## Can I use it with an ORM like ActiveRecord or Sequel?

Not yet, but you are welcome to contribute adapters for those projects. I will
be releasing my own not-an-ORM tool in the near future.
