# Extralite - a Super Fast Ruby Gem for Working with SQLite3 Databases

* Source code: https://github.com/digital-fabric/extralite
* Documentation: http://www.rubydoc.info/gems/extralite

[![Ruby gem](https://badge.fury.io/rb/extralite.svg)](https://rubygems.org/gems/extralite) [![Tests](https://github.com/digital-fabric/extralite/workflows/Tests/badge.svg)](https://github.com/digital-fabric/extralite/actions?query=workflow%3ATests) [![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/digital-fabric/extralite/blob/master/LICENSE)

## What is Extralite?

Extralite is a super fast, extra-lightweight (about 1300 lines of C-code)
SQLite3 wrapper for Ruby. It provides a minimal set of methods for interacting
with an SQLite3 database, as well as prepared queries (prepared statements).

Extralite comes in two flavors: the `extralite` gem which uses the
system-installed sqlite3 library, and the `extralite-bundle` gem which bundles
the latest version of SQLite
([3.44.2](https://sqlite.org/releaselog/3_44_2.html)), offering access to the
latest features and enhancements.

## Features

- Super fast - [up to 11x faster](#performance) than the
  [sqlite3](https://github.com/sparklemotion/sqlite3-ruby) gem (see also
  [comparison](#why-not-just-use-the-sqlite3-gem).)
- A variety of methods for different data access patterns: rows as hashes, rows
  as arrays, single row, single column, single value.
- Prepared statements.
- Parameter binding.
- External iteration - get single records or batches of records.
- Use system-installed sqlite3, or the [bundled latest version of
  SQLite3](#installing-the-extralite-sqlite3-bundle).
- Improved [concurrency](#concurrency) for multithreaded apps: the Ruby GVL is
  released while preparing SQL statements and while iterating over results.
- Automatically execute SQL strings containing multiple semicolon-separated
  queries (handy for creating/modifying schemas).
- Execute the same query with multiple parameter lists (useful for inserting records).
- Load extensions (loading of extensions is autmatically enabled. You can find
  some useful extensions here: https://github.com/nalgeon/sqlean.)
- Includes a [Sequel adapter](#usage-with-sequel).

## Installation

To use Extralite in your Ruby app, add the following to your `Gemfile`:

```ruby
gem 'extralite'
```

You can also run `gem install extralite` if you just want to check it out.

### Installing the Extralite-SQLite3 Bundle

If you don't have sqlite3 installed on your system, do not want to use the
system-installed version of SQLite3, or would like to use the latest version of
SQLite3, you can install the `extralite-bundle` gem, which integrates the
SQLite3 source code.

> **Important note**: The `extralite-bundle` gem will take a while to install
> (on my modest machine it takes about a minute), due to the size of the sqlite3
> code.

Usage of the `extralite-bundle` gem is identical to the usage of the normal
`extralite` gem, using `require 'extralite'` to load the gem.

## Synopsis

```ruby
require 'extralite'

# get sqlite3 version
Extralite.sqlite3_version #=> "3.35.2"

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
db.query_hash('select ?2 as foo, ?1 as bar, ?1 * ?2 as baz', 6, 7) #=> [{ :foo => 7, :bar => 6, :baz => 42 }]

# parameter binding of named parameters
db.query('select * from foo where bar = :bar', bar: 42)
db.query('select * from foo where bar = :bar', 'bar' => 42)
db.query('select * from foo where bar = :bar', ':bar' => 42)

# parameter binding of named parameters from Struct and Data
SomeStruct = Struct.new(:foo, :bar)
db.query_single_column('select :bar', SomeStruct.new(41, 42)) #=> [42]
SomeData = Data.define(:foo, :bar)
db.query_single_column('select :bar', SomeData.new(foo: 41, bar: 42)) #=> [42]

# parameter binding for binary data (BLOBs)
db.execute('insert into foo values (?)', File.binread('/path/to/file'))
db.execute('insert into foo values (?)', Extralite::Blob.new('Hello, 世界!'))
db.execute('insert into foo values (?)', 'Hello, 世界!'.force_encoding(Encoding::ASCII_8BIT))

# insert multiple rows
db.execute_multi('insert into foo values (?)', ['bar', 'baz'])
db.execute_multi('insert into foo values (?, ?)', [[1, 2], [3, 4]])

# prepared queries
query = db.prepare('select ? as foo, ? as bar') #=> Extralite::Query
query.bind(1, 2) #=> [{ :foo => 1, :bar => 2 }]

query.next #=> next row in result_set (as hash)
query.next_hash #=> next row in result_set (as hash)
query.next_ary #=> next row in result_set (as array)
query.next_single_column #=> next row in result_set (as single value)

query.next(10) #=> next 10 rows in result_set (as hash)
query.next_hash(10) #=> next 10 rows in result_set (as hash)
query.next_ary(10) #=> next 10 rows in result_set (as array)
query.next_single_column(10) #=> next 10 rows in result_set (as single value)

query.to_a #=> all rows as array of hashes
query.to_a_hash #=> all rows as array of hashes
query.to_a_ary #=> all rows as array of arrays
query.to_a_single_column #=> all rows as array of single values

query.each { |r| ... } #=> iterate over all rows as hashes
query.each_hash { |r| ... } #=> iterate over all rows as hashes
query.each_ary { |r| ... } #=> iterate over all rows as arrays
query.each_single_column { |r| ... } #=> iterate over all rows as single columns

iterator = query.each #=> create enumerable iterator
iterator.next #=> next row
iterator.each { |r| ... } #=> iterate over all rows
values = iterator.map { |r| r[:foo] * 10 } #=> map all rows

iterator = query.each_ary #=> create enumerable iterator with rows as arrays
iterator = query.each_single_column #=> create enumerable iterator with single values

# get last insert rowid
rowid = db.last_insert_rowid

# get number of rows changed in last query
number_of_rows_affected = db.changes

# get column names for the given sql
db.columns('select a, b, c from foo') => [:a, :b, :c]

# get db filename
db.filename #=> "/tmp/my.db"

# get list of tables
db.tables #=> ['foo', 'bar']

# get and set pragmas
db.pragma(:journal_mode) #=> 'delete'
db.pragma(journal_mode: 'wal')
db.pragma(:journal_mode) #=> 'wal'

# load an extension
db.load_extension('/path/to/extension.so')

# run queries in a transaction
db.transaction do
  db.execute('insert into foo values (?)', 1)
  db.execute('insert into foo values (?)', 2)
  db.execute('insert into foo values (?)', 3)
end

# close database
db.close
db.closed? #=> true
```

## More Features

### Interrupting Long-running Queries

When running long-running queries, you can use `Database#interrupt` to interrupt
the query:

```ruby
timeout_thread = Thread.new do
  sleep 10
  db.interrupt
end

result = begin
  db.query(super_slow_sql)
rescue Extralite::InterruptError
  nil
ensure
  timeout_thread.kill
  timeout_thread.join
end
```

### Running transactions

In order to run multiple queries in a single transaction, use
`Database#transaction`, passing a block that runs the queries. You can specify
the [transaction
mode](https://www.sqlite.org/lang_transaction.html#deferred_immediate_and_exclusive_transactions).
The default mode is `:immediate`:

```ruby
db.transaction { ... } # Run an immediate transaction
db.transaction(:deferred) { ... } # Run a deferred transaction
db.transaction(:exclusive) { ... } # Run an exclusive transaction
```

If an exception is raised in the given block, the transaction will be rolled
back. Otherwise, it is committed.

### Creating Backups

You can use `Database#backup` to create backup copies of a database. The
`#backup` method takes either a filename or a database instance:

```ruby
# with a filename
db.backup('backup.db')

# with an instance
target = Extralite::Database.new('backup.db')
db.backup(target)
```

For big databases, you can also track the backup progress by providing a block
that takes two arguments - the number of remaining pages, and the total number pages:

```ruby
db.backup('backup.db') do |remaining, total|
  puts "backup progress: #{(remaining.to_f/total * 100).round}%"
end
```

### Retrieving Status Information

Extralite provides methods for retrieving status information about the sqlite
runtime, database-specific status and prepared statement-specific status,
`Extralite.runtime_status`, `Database#status` and `Query#status` respectively.
You can also reset the high water mark for the specific status code by providing
true as the reset argument. The status codes mirror those defined by the SQLite
API. Some examples:

```ruby
# The Extralite.runtime_status returns a tuple consisting of the current value
# and the high water mark value.
current, high_watermark = Extralite.runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED)

# To reset the high water mark, pass true as a second argument:
current, high_watermark = Extralite.runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED, true)

# Similarly, you can interrogate a database's status (pass true as a second
# argument in order to reset the high watermark):
current, high_watermark = db.status(Extralite::SQLITE_DBSTATUS_CACHE_USED)

# The Query#status method returns a single value (pass true as a
# second argument in order to reset the high watermark):
value = query.status(Extralite::SQLITE_STMTSTATUS_RUN)
```

### Working with Database Limits

The `Database#limit` can be used to get and set various database limits, as
[discussed in the SQLite docs](https://www.sqlite.org/limits.html):

```ruby
# get limit
value = db.limit(Extralite::SQLITE_LIMIT_ATTACHED)

# set limit
db.limit(Extralite::SQLITE_LIMIT_ATTACHED, new_value)
```

### Setting the Busy Timeout

When accessing a database concurrently it can be handy to set a busy timeout, in
order to not have to deal with rescuing `Extralite::BusyError` exceptions. The
timeout is given in seconds:

```ruby
db.busy_timeout = 5
```

### Tracing SQL Statements

To trace all SQL statements executed on the database, pass a block to
`Database#trace`. To disable tracing, call `Database#trace` without a block:

```ruby
# enable tracing
db.trace { |sql| puts sql: sql }

# disable tracing
db.trace
```

## Usage with Sequel

Extralite includes an adapter for
[Sequel](https://github.com/jeremyevans/sequel). To use the Extralite adapter,
just use the `extralite` scheme instead of `sqlite`:

```ruby
DB = Sequel.connect('extralite://blog.db')
articles = DB[:articles]
p articles.to_a
```

(Make sure you include `extralite` as a dependency in your `Gemfile`.)

## Concurrency

Extralite releases the GVL while making blocking calls to the sqlite3 library,
that is while preparing SQL statements and fetching rows. Releasing the GVL
allows other threads to run while the sqlite3 library is busy compiling SQL into
bytecode, or fetching the next row. This *does not* hurt Extralite's
performance, as you can see:

## Performance

A benchmark script is included, creating a table of various row counts, then
fetching the entire table using either `sqlite3` or `extralite`. This benchmark
shows Extralite to be up to ~11 times faster than `sqlite3` when fetching a
large number of rows.

### Rows as Hashes

 [Benchmark source code](https://github.com/digital-fabric/extralite/blob/main/test/perf_hash.rb)

|Row count|sqlite3 1.6.0|Extralite 1.21|Advantage|
|-:|-:|-:|-:|
|10|63.7K rows/s|94.0K rows/s|__1.48x__|
|1K|299.2K rows/s|1.983M rows/s|__6.63x__|
|100K|185.4K rows/s|2.033M rows/s|__10.97x__|

### Rows as Arrays

[Benchmark source code](https://github.com/digital-fabric/extralite/blob/main/test/perf_ary.rb)

|Row count|sqlite3 1.6.0|Extralite 1.21|Advantage|
|-:|-:|-:|-:|
|10|71.2K rows/s|92.1K rows/s|__1.29x__|
|1K|502.1K rows/s|2.065M rows/s|__4.11x__|
|100K|455.7K rows/s|2.511M rows/s|__5.51x__|

### Prepared Queries (Prepared Statements)

[Benchmark source code](https://github.com/digital-fabric/extralite/blob/main/test/perf_prepared.rb)

|Row count|sqlite3 1.6.0|Extralite 1.21|Advantage|
|-:|-:|-:|-:|
|10|232.2K rows/s|741.6K rows/s|__3.19x__|
|1K|299.8K rows/s|2386.0M rows/s|__7.96x__|
|100K|183.1K rows/s|1.893M rows/s|__10.34x__|

As those benchmarks show, Extralite is capabale of reading up to 2.5M
rows/second when fetching rows as arrays, and up to 2M rows/second when fetching
rows as hashes.

## License

The source code for Extralite is published under the [MIT license](LICENSE). The
source code for SQLite is in the [public
domain](https://sqlite.org/copyright.html).

## Contributing

Contributions in the form of issues, PRs or comments will be greatly
appreciated!
