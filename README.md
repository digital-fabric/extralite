# Extralite - a Ruby gem for working with SQLite3 databases

[![Gem Version](https://badge.fury.io/rb/extralite.svg)](http://rubygems.org/gems/extralite)
[![Modulation Test](https://github.com/digital-fabric/extralite/workflows/Tests/badge.svg)](https://github.com/digital-fabric/extralite/actions?query=workflow%3ATests)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/digital-fabric/extralite/blob/master/LICENSE)

## What is Extralite?

Extralite is an extra-lightweight (less than 430 lines of C-code) SQLite3
wrapper for Ruby. It provides a single class with a minimal set of methods to
interact with an SQLite3 database.

## Features

- A variety of methods for different data access patterns: rows as hashes, rows
  as arrays, single row, single column, single value.
- Super fast - [up to 12.5x faster](#performance) than the
  [sqlite3](https://github.com/sparklemotion/sqlite3-ruby) gem (see also
  [comparison](#why-not-just-use-the-sqlite3-gem).)
- Improved [concurrency](#what-about-concurrency) for multithreaded apps: the
  Ruby GVL is released while preparing SQL statements and while iterating over
  results.
- Iterate over records with a block, or collect records into an array.
- Parameter binding.
- Automatically execute SQL strings containing multiple semicolon-separated
  queries (handy for creating/modifying schemas).
- Get last insert rowid.
- Get number of rows changed by last query.
- Load extensions (loading of extensions is autmatically enabled. You can find
  some useful extensions here: https://github.com/nalgeon/sqlean.)
- Includes a [Sequel adapter](#usage-with-sequel) (an ActiveRecord)

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

# parameter binding of named parameters
db.query('select * from foo where bar = :bar', bar: 42)
db.query('select * from foo where bar = :bar', 'bar' => 42)
db.query('select * from foo where bar = :bar', ':bar' => 42)

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

## Usage with Sequel

Extralite includes an adapter for
[Sequel](https://github.com/jeremyevans/sequel). To use the Extralite adapter,
just use the `extralite` scheme instead of `sqlite`:

```ruby
DB = Sequel.connect('extralite:blog.db')
articles = DB[:articles]
p articles.to_a
```

(Make sure you include `extralite` as a dependency in your `Gemfile`.)

## Why not just use the sqlite3 gem?

The [sqlite3-ruby](https://github.com/sparklemotion/sqlite3-ruby) gem is a
popular, solid, well-maintained project, used by thousands of developers. I've
been doing a lot of work with SQLite3 databases lately, and wanted to have a
simpler API that gives me query results in a variety of ways. Thus extralite was
born.

Extralite is quite a bit [faster](#performance) than sqlite3-ruby and is also
[thread-friendly](#what-about-concurrency). On the other hand, Extralite does
not have support for defining custom functions, aggregates and collations. If
you're using those features, you'll need to stick with sqlite3-ruby.

Here's a table summarizing the differences between the two gems:

| |sqlite3-ruby|Extralite|
|-|-|-|
|API design|multiple classes|single class|
|Query results|row as hash, row as array, single row, single value|row as hash, row as array, __single column__, single row, single value|
|execute multiple statements|separate API (#execute_batch)|integrated|
|custom functions in Ruby|yes|no|
|custom collations|yes|no|
|custom aggregate functions|yes|no|
|Multithread friendly|no|[yes](#what-about-concurrency)|
|Code size|~2650LoC|~500LoC|
|Performance|1x|1.5x to 12.5x (see [below](#performance))|

## What about concurrency?

Extralite releases the GVL while making blocking calls to the sqlite3 library,
that is while preparing SQL statements and fetching rows. Releasing the GVL
allows other threads to run while the sqlite3 library is busy compiling SQL into
bytecode, or fetching the next row. This does not seem to hurt Extralite's
performance:

## Performance

A benchmark script is
[included](https://github.com/digital-fabric/extralite/blob/main/test/perf.rb),
creating a table of various row counts, then fetching the entire table using
either `sqlite3` or `extralite`. This benchmark shows Extralite to be up to 12.5
times faster than `sqlite3` when fetching a large number of rows. Here are the
results (using the `sqlite3` gem performance as baseline):

|Row count|sqlite3-ruby (baseline)|Extralite (relative - rounded)|
|-:|-:|-:|
|10|1x|1.5x|
|1K|1x|7x|
|100K|1x|12.5x|

(If you're interested in checking this yourself, just run the script and let me
know if your results are different.)

## Contributing

Contributions in the form of issues, PRs or comments will be greatly
appreciated!