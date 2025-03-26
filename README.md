<h1 align="center">
  <br>
  Extralite
</h1>

<h4 align="center">Ruby on SQLite</h4>

<p align="center">
  <a href="http://rubygems.org/gems/extralite">
    <img src="https://badge.fury.io/rb/extralite.svg" alt="Ruby gem">
  </a>
  <a href="https://github.com/digital-fabric/extralite/actions">
    <img src="https://github.com/digital-fabric/extralite/actions/workflows/test.yml/badge.svg" alt="Tests">
  </a>
  <a href="https://github.com/digital-fabric/extralite/blob/master/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License">
  </a>
</p>

<p align="center">
  <a href="https://www.rubydoc.info/gems/extralite">API reference</a>
</p>

## What is Extralite?

Extralite is a fast and innovative SQLite wrapper for Ruby with a rich set of
features. It provides multiple ways of retrieving data from SQLite databases,
makes it possible to use SQLite databases in multi-threaded and multi-fibered
Ruby apps, and includes a comprehensive set of tools for managing SQLite
databases.

Extralite comes in two flavors: the `extralite` gem which uses the
system-installed sqlite3 library, and the `extralite-bundle` gem which bundles
the latest version of SQLite
([3.49.0](https://sqlite.org/releaselog/3_49_0.html)), offering access to the
latest features and enhancements.

## Features

- Best-in-class [performance](#performance) (up to 4.5X the performance of the
  [sqlite3](https://github.com/sparklemotion/sqlite3-ruby) gem).
- Support for [concurrency](#concurrency) out of the box for multi-threaded
  and multi-fibered apps.
- A variety of ways to [retrieve data](#query-modes) - hashes, arrays, single
  columns, single rows, [transforms](#value-transforms).
- Support for [external iteration](#iterating-over-records-in-a-prepared-query),
  allowing iterating through single records or batches of records.
- [Prepared queries](#prepared-queries).
- [Parameter binding](#parameter-binding).
- [Batch execution](#batch-execution-of-queries) of queries.
- [transactions and savepoints](#transactions-and-savepoints).
- Advanced features: load [SQLite extensions](#loading-extensions), create
  [backups](#creating-backups), retrieve [status
  information](#retrieving-status-information), work with
  [changesets](#working-with-changesets), interrogate [database
  limits](#working-with-database-limits),  [trace](#tracing-sql-statements)
  queries.
- [Sequel](#usage-with-sequel) adapter.

## Table of Content

- [Installing Extralite](#installing-extralite)
- [Getting Started](#getting-started)
- [Query Modes](#query-modes)
- [Parameter binding](#parameter-binding)
- [Value Transforms](#value-transforms)
- [Data Types](#data-types)
- [Prepared Queries](#prepared-queries)
- [Batch Execution of Queries](#batch-execution-of-queries)
- [Transactions and Savepoints](#transactions-and-savepoints)
- [Database Information](#database-information)
- [Error Handling](#error-handling)
- [Concurrency](#concurrency)
- [Advanced Usage](#advanced-usage)
- [Usage with Sequel](#usage-with-sequel)
- [Performance](#performance)
- [License](#license)
- [Contributing](#contributing)

## Installing Extralite

Using bundler:

```ruby
gem 'extralite'
```

Or manually:

```bash
$ gem install extralite
```

__Note__: Extralite supports Ruby 3.0 and newer.

### Installing the Extralite-SQLite Bundle

If you don't have the sqlite3 lib installed on your system, do not want to use
the system-installed version of SQLite, or would like to use the latest version
of SQLite, you can install the `extralite-bundle` gem, which integrates the
SQLite source code.

Usage of the `extralite-bundle` gem is identical to the usage of the normal
`extralite` gem, using `require 'extralite'` to load the gem.

## Getting Started

The following example shows how to open an SQLite database and run some queries:

```ruby
db = Extralite::Database.new('mydb.sqlite')
db.execute('create table foo (x, y, z)')
db.execute('insert into foo values (?, ?, ?)', 1, 2, 3)
db.execute('insert into foo values (?, ?, ?)', 4, 5, 6)
db.query('select * from foo') #=> [{x: 1, y: 2, z: 3}, {x: 4, y: 5, z: 6}]
```

The `#execute` method is used to make changes to the database, such as creating
tables, or inserting records. It returns the number of records changed by the
query.

The `#query` method is used to read data from the database. It returns an array
containing the resulting records, represented as hashes mapping column names (as
symbols) to individual values.

You can also iterate on records by providing a block to `#query`:

```ruby
db.query 'select * from foo' do |r|
  p record: r
end
```

## Query Modes

Extralite allows you to retrieve data from SQLite database in the form that most
a particular context. For some use cases you'll want to work with rows as
hashes. In other cases, you'll want to work with rows as arrays, or even as
single values, if you're just reading one column.

For that purpose, Extralite offers three different ways, or modes, of retrieving
records:

- `:hash`: retrieve each row as a hash (this is the default mode).
- `:array`: retrieve each row as an array.
- `:splat`: retrieve each row as one or more splatted values, without wrapping
  them in a container (see [below](#the-splat-query-mode)).

Extralite provides separate methods for the different modes:

```ruby
# alias #query_hash
db.query('select 1') #=> [{ "1" => 1 }]

db.query_array('select 1') #=> [[1]]

db.query_splat('select 1') #=> [1]
```

Notice how all the return values above are arrays. This is because the different
`#query_xxx` methods are designed to return multiple rows. If you want to just
get back a single row, use one of the `query_single_xxx` methods:

```ruby
# alias #query_single_hash
db.query_single('select 1') #=> { "1" => 1 }

db.query_single_array('select 1') #=> [1]

db.query_single_splat('select 1') #=> 1
```

### Iterating Over Query Results with a Block

In addition to getting query results as an array of rows, you can also directly
iterate over the query results by providing a block to the different
`#query_xxx` methods:

```ruby
db.query('select * from foo') { |row| handle_row(row) }
```

### The Splat Query Mode

The splat query mode allows you to retrieve column values for each row without
wrapping them in a container. This is useful especially when performing queries
that return a single column:

```ruby
# When using the array mode we need to take unwrap the values
ids = db.query_array('select id from tasks where active = 1').map { |r| r.first }

# In splat mode we don't need to do that
ids = db.query_splat('select id from tasks where active = 1')
```

The splat mode is also useful when iterating over records with a block. The
column values are provided as splatted arguments to the given block:

```ruby
db.query_splat('select a, b, c from foo') do |a, b, c|
  do_this_with(a, b)
  do_that_with(c)
end
```

When iterating over records in this manner, the splat mode is slightly faster
than the array mode, and also reduces pressure on the Ruby GC since you avoid
allocating arrays or hashes to hold the column values.

## Parameter Binding

The `#execute` and `#query_xxx` methods accept parameters that can be bound to
the query, which means that their values will be used for each corresponding
place-holder (expressed using `?`) in the SQL statement:

```ruby
db.query('select x from my_table where y = ? and z = ?', 'foo', 'bar')
```

You can also express place holders by specifying their index (starting from 1)
using `?IDX`:

```ruby
# use the same value for both place holders:
db.query('select x from my_table where y = ?1 and z = ?1', 42)
```

Another possibility is to use named parameters, which can be done by expressing
place holders as `:KEY`, `@KEY` or `$KEY`:

```ruby
db.query('select x from my_table where y = $y and z = $z', y: 'foo', z: 'bar')
```

Extralite supports specifying named parameters using `Struct` or `Data` objects:

```ruby
MyStruct = Struct.new(:x, :z)
params = MyStruct.new(42, 6)
db.execute('update foo set x = $x where z = $z', params)

MyData = Data.define(:x, :z)
params = MyData.new(43, 3)
db.execute('update foo set x = $x where z = $z', params)
```

Parameter binding is especially useful for preventing [SQL-injection
attacks](https://en.wikipedia.org/wiki/SQL_injection), but is also useful when
combined with [prepared queries](#prepared-queries) when repeatedly running the
same query over and over.

## Data Types

Extralite supports the following data types for either bound parameters or row
values:

- `Integer`
- `Float`
- `Boolean` (see below)
- `String` (see below)
- nil

### Boolean Values

SQLite does not have a boolean data type. Extralite will automatically translate
bound parameter values of `true` or `false` to the integer values `1` and `0`,
respectively. Note that boolean values stored in the database will be fetched as
integers.

### String Values

String parameter values are translated by Extralite to either `TEXT` or `BLOB`
values according to the string encoding used. Strings with an `ASCII-8BIT` are
treated as blobs. Otherwise they are treated as text values.

Likewise, when fetching records, Extralite will convert a `BLOB` column value to
a string with `ASCII-8BIT` encoding, and a `TEXT` column value to a string with
`UTF-8` encoding.

```ruby
# The following calls will insert blob values into the database
sql = 'insert into foo values (?)'
db.execute(sql, File.binread('/path/to/file'))
db.execute(sql, Extralite::Blob.new('Hello, 世界!'))
db.execute(sql, 'Hello, 世界!'.force_encoding(Encoding::ASCII_8BIT))
```

## Value Transforms

Extralite allows you to transform rows to any value your application may need by
providing a transform proc that takes the raw row values and returns the
transformed data. The transform proc is passed each resulting row either as a
hash or as a list of values.

Transforms are useful when you need to transform rows into ORM model instances,
or when you need to do some other transformation on the values retrieved from
the database.

To transform results, pass a transform proc as the first parameter to one of the
`#query_xxx` methods:

```ruby
transform = ->(h) { MyModel.new(h) }
db.query(transform, 'select * from foo')
#=> rows as instances of MyModel
```

When using the `splat` mode, the different column values are passed as splatted
values to the transform proc:

```ruby
transform = ->(a, b, c) { { a:a, b: b, c: JSON.parse(c) } }
db.query_splat(transform, 'select a, b, c from foo')
#=> transformed rows
```

Value transforms can also be done with [prepared
queries](#value-transforms-in-prepared-queries).

## Prepared Queries

Prepared queries (also known as prepared statements) allow you to maximize
performance and reduce memory usage when running the same query repeatedly. They
also allow you to create parameterized queries that can be repeatedly executed
with different parameters:

```ruby
query = db.prepare('select * from foo where x = ?')

# bind parameters and get results as an array of hashes
query.bind(1).to_a
#=> [{ x: 1, y: 2, z: 3 }]
```

### Binding Values to Prepared Queries

To bind parameter values to the query, use the `#bind` method. The parameters
will remain bound to the query until `#bind` is called again.

```ruby
query.bind(1)

# run the query any number of times
3.times { query.to_a }

# bind other parameter values
query.bind(4)
```

You can also bind parameters when creating the prepared query by passing
additional parameters to the `Database#prepare` method:

```ruby
query = db.prepare('select * from foo where x = ?', 1)
```

### Fetching Records from a Prepared Query

Just like the `Database` interface, prepared queries support getting data using
three different modes: as a hash, an array or as individual column values. To
set the mode, you can use one of the `#prepare_xxx` methods:

```ruby
# hash mode (alias #prepare_hash)
db.prepare('select * from foo').to_a
#=> [{ x: 1, y: 2, z: 3}]

# splat mode
db.prepare_splat('select x from foo').to_a
#=> [1]

# array mode
db.prepare_array('select * from foo').to_a
#=> [[1, 2, 3]]
```

You can also set the query mode by getting or setting `#mode`:

```ruby
q = db.prepare('select * from foo')
q.to_a #=> [{ x: 1, y: 2, z: 3}]

q.mode #=> :hash
q.mode = :array
q.to_a "=> [[1, 2, 3]]
```

### Fetching Single Records or Batches of Records

Prepared queries let you iterate over records one by one, or by batches. For
this, use the `#next` method:

```ruby
query = db.prepare('select * from foo')

query.next
#=> { x: 1, y: 2, z: 3 }
query.next
#=> { x: 4, y: 5, z: 6 }

# Fetch the next 10 records
query.reset # go back tpo the beginning
query.next(10)
#=> [{ x: 1, y: 2, z: 3 }, { x: 4, y: 5, z: 6 }]

# Fetch the next row as an array
query = db.prepare_array('select * from foo')
query.next
#=> [1, 2, 3]

# Fetch the next row as a single column
db.prepare_splat('select z from foo').next
#=> 3
```

To detect the end of the result, you can use `#eof?`. To go back to the
beginning of the result set, use `#reset`. The following example shows how to
read the query results in batches of 10:

```ruby
query.reset
while !query.eof?
  records = query.next(10)
  process_records(records)
end
```

### Iterating over Records in a Prepared Query

In addition to the `#next` method, you can also iterate over query results by
using the familiar `#each` method:

```ruby
# iterate over records as hashes
query = db.prepare('select * from foo')
query.each { |r| ... }

# iterate over records as arrays
query = db.prepare_array('select * from foo')
query.each { |r| ... }

# iterate over records as single values
query = db.prepare_splat('select a, b, c from foo')
query.each { |a, b, c| ... }
```

### Prepared Query as an Enumerable

You can also use a prepared query as an enumerable, allowing you to chain
enumerable method calls while iterating over the query result set. This is done
by calling `#each` without a block:

```ruby
iterator = query.each
#=> Returns an Extralite::Iterator instance

iterator.map { |r| r[:x] *100 + r[:y] * 10 + r[:z] }
#=> [123, 345]
```

The Iterator class includes the
[`Enumerable`](https://rubyapi.org/3.3/o/enumerable) module, with all its
methods, such as `#map`, `#select`, `#inject`, `#lazy` etc. You can also
instantiate an iterator explicitly:

```ruby
# You need to pass the query to iterate over:
iterator = Extralite::Iterator.new(query)
iterator.each { |r| ... }
```

### Value Transforms in Prepared Queries

Prepared queries can automatically transform their result sets by setting a
transform block. The transform block receives values according to the query mode
(hash, array or splat). To set a transform you can pass a block to one of the
`Database#prepare_xxx` methods, or use `Query#transform`:

```ruby
q = db.prepare('select * from items where id = ?') { |h| Item.new(h) }
q.bind(42).next #=> Item instance

# An equivalent
q = db.prepare('select * from items where id = ?')
q.transform { |h| Item.new(h) }
```

The same can be done for queries in `splat` or `array` mode:

```ruby
db.prepare_splat('select * from foo') { |a, b, c| a + b + c }

db.prepare_array('select * from foo') { |a| a.map(&:to_s).join }
```

## Batch Execution of Queries

Extralite provides methods for batch execution of queries, with multiple sets of
parameters. The `#batch_execute` method lets you insert or update a large number
of records with a single call:

```ruby
values = [
  [1, 11, 111],
  [2, 22, 222],
  [3, 33, 333]
]
# insert the above records in one fell swoop, and returns the total number of
# changes:
db.batch_execute('insert into foo values (?, ?, ?)', values)
#=> 3
```

Parameters to the query can also be provided by any object that is an
`Enumerable` or has an `#each` method, or any *callable* object that responds to
`#call`:

```ruby
# Take parameter values from a Range
db.batch_execute('insert into foo values (?)', 1..10)
#=> 10

# Insert (chomped) lines from a file
File.open('foo.txt') do |f|
  source = f.each_line.map(&:chomp)
  db.batch_execute('insert into foo values (?)', source)
end

# Insert items from a queue
parameters = proc do
  item = queue.shift
  # when we're done, we return nil
  (item == :done) ? nil : item
end
db.batch_execute('insert into foo values (?)', parameters)
#=> number of rows inserted
```

Like its cousin `#execute`, the `#batch_execute` returns the total number of
changes to the database (rows inserted, deleted or udpated).

### Batch Execution of Queries that Return Rows

Extralite also provides a `#batch_query` method that like `#batch_execute` takes
a parameter source and returns an array containing the result sets for all query
invocations. If a block is given, the result sets are passed to the block
instead.

The `#batch_query` method is especially useful for batch queries with a
`RETURNING` clause:

```ruby
updates = [
  { id: 3, price: 42 },
  { id: 5, price: 43 }
]
sql = 'update foo set price = $price where id = $id returning id, quantity'
db.batch_query(sql, updates)
#=> [[{ id: 3, quantity: 4 }], [{ id: 5, quantity: 5 }]]

# The same with a block (returns the total number of changes)
db.batch_query(sql, updates) do |rows|
  p rows
  #=> [{ id: 3, quantity: 4 }]
  #=> [{ id: 5, quantity: 5 }]
end
#=> 2
```

The `#batch_query` method, like other row fetching methods, changes the row
representation according to the query mode.

### Batch Execution of Prepared Queries

Batch execution can also be done using prepared queries, using the same methods
`#batch_execute` and `#batch_query`:

```ruby
query = db.prepare 'update foo set x = ? where z = ? returning *'

query.batch_execute([[42, 3], [43, 6]])
#=> 2

query.batch_query([[42, 3], [43, 6]])
#=> [{ x: 42, y: 2, z: 3 }, { x: 43, y: 5, z: 6 }]
```

## Transactions and Savepoints

All reads and writes to SQLite databases occur within a
[transaction](https://www.sqlite.org/lang_transaction.html). If no explicit
transaction has started, the submitted SQL statements passed to `#execute` or
`#query` will all run within an implicit transaction:

```ruby
# The following two SQL statements will run in a single implicit transaction:
db.execute('insert into foo values (42); insert into bar values (43)')

# Otherwise, each call to #execute runs in a separate transaction:
db.execute('insert into foo values (42)')
db.execute('insert into bar values (43)')
```

### Explicit Transactions

While you can issue `BEGIN` and `COMMIT` SQL statements yourself to start and
commit explicit transactions, Extralite provides a convenient `#transaction`
method that manages starting, commiting and rolling back of transactions
automatically:

```ruby
db.transaction do
  db.execute('insert into foo values (42)')
  raise 'Something bad happened' if something_bad_happened
  db.execute('insert into bar values (43)')
end
```

If no exception is raised in the transaction block, the changes are commited. If
an exception is raised, the changes are rolled back and the exception is
propagated to the application code. You can prevent the exception from being
propagated by calling `#rollback!`:

```ruby
db.transaction do
  db.execute('insert into foo values (42)')
  rollback! if something_bad_happened
  db.execute('insert into bar values (43)')
end
```

### Transaction Modes

By default, `#transaction` starts an `IMMEDIATE` transaction. To start a
`DEFERRED` or `EXCLUSIVE` transaction, pass the desired mode to `#transaction`:

```ruby
# Start a DEFERRED transaction
db.transaction(:deferred) do
  ...
end

# Start a EXCLUSIVE transaction
db.transaction(:exclusive) do
  ...
end
```

Note that running an `IMMEDIATE` or `EXCLUSIVE` transaction blocks the database
for writing (and also reading in certain cases) for the duration of the
transaction. This can cause queries to the same database on a different
connection to fail with a `BusyError` exception. This can be mitigated by
setting a [busy timeout](#dealing-with-a-busy-database).

### Savepoints

In addition to transactions, SQLite also supports the use of
[savepoints](https://www.sqlite.org/lang_savepoint.html), which can be used for
more fine-grained control of changes within a transaction, and to be able to
rollback specific changes without abandoning the entire transaction:

```ruby
db.transaction do
  db.execute 'insert into foo values (1)'
  
  db.savepoint :my_savepoint
  db.execute 'insert into foo values (2)'
  
  # the following cancels the last insert
  db.rollback_to :my_savepoint
  db.execute 'insert into foo values (3)'

  db.release :my_savepoint
end
```

## Database Information

### Getting the List of Tables

To get the list of tables in a database, use the `#tables` method:

```ruby
db.tables
#=> [...]
```

To get the list of tables in an attached database, you can pass the database name to `#tables`:

```ruby
db.execute "attach database 'foo.db' as foo"
db.tables('foo')
#=> [...]
```

### Getting the Last Insert Row Id

```ruby
db.execute 'insert into foo values (?)', 42
db.last_insert_rowid
#=> 1
```

### Getting the Column Names for a Given Query

```ruby
db.columns('select a, b, c from foo')
#=> [:a, :b, :c]

# Columns a prepared query:
query = db.prepare('select x, y from foo')
query.columns
#=> [:x, :y]
```

### Pragmas

You can get or set pragma values using `#pragma`:

```ruby
# get a pragma value:
db.pragma(:journal_mode)
#=> 'delete'

# set a pragma value:
db.pragma(journal_mode: 'wal')
db.pragma(:journal_mode)
#=> 'wal'
```

You can also pass pragmas when opening the database:

```ruby
db = Extralite::Database.new('path/to/db', pragma: { foreign_keys: true })
```

## Error Handling

Extralite defines various exception classes that are raised when an error is
encountered while interacting with the underlying SQLite library:

- `Extralite::SQLError` - raised when SQLite encounters an invalid SQL query.
- `Extralite::BusyError` - raised when the underlying database is locked for use
  by another database connection.
- `Extralite::InterruptError` - raised when a query has been interrupted.
- `Extralite::ParameterError` - raised when an invalid parameter value has been
  specified.
- `Extralite::Error` - raised on all other errors.

In addition to the above exceptions, further information about the last error
that occurred is provided by the following methods:

- `#errcode` - the [error code](https://www.sqlite.org/rescode.html) returned by
  the underlying SQLite library.
- `#errmsg` - the error message for the last error. For most errors, the error
  message is copied into the exception message.
- `#error_offset` - for SQL errors, the offset into the SQL string where the
  error was encountered.

## Concurrency

Extralite provides a comprehensive set of tools for dealing with concurrency
issues, and for making sure that running queries on SQLite databases does not
cause the app to freeze.

**Note**: In order to allow concurrent access your the database, it is highly
recommended that you set your database to use [WAL journaling
mode](https://www.sqlite.org/wal.html) for *all* database connections.
Otherwise, you risking running into performance problems and having queries fail
with `BusyError` exceptions. You can easily open your database in WAL journaling
mode by passing a `wal: true` option:

```ruby
# This will set PRAGMA journal_mode=1 and PRAGMA synchronous=1
db = Extralite::Database.new('path/to/db', wal: true)
```

### The Ruby GVL

In order to support multi-threading, Extralite releases the [Ruby
GVL](https://www.speedshop.co/2020/05/11/the-ruby-gvl-and-scaling.html)
periodically while running queries. This allows other threads to run while the
underlying SQLite library is busy preparing queries, fetching records and
backing up databases. By default, the GVL is when preparing the query, and once
for every 1000 iterated records. The GVL release threshold can be set separately
for each database:

```ruby
# release the GVL on preparing the query, and every 10 records
db.gvl_release_threshold = 10 

# release the GVL only when preparing the query
db.gvl_release_threshold = 0 

# never release the GVL (for single-threaded apps only)
db.gvl_release_threshold = -1

db.gvl_release_threshold = nil # use default value (currently 1000)
```

For most applications, there's no need to tune the GVL threshold value, as it
provides [excellent](#performance) performance characteristics for both
single-threaded and multi-threaded applications.

In a heavily multi-threaded application, releasing the GVL more often (lower
threshold value) will lead to less latency (for threads not running a query),
but will also hurt the throughput (for the thread running the query). Releasing
the GVL less often (higher threshold value) will lead to better throughput for
queries, while increasing latency for threads not running a query. The following
diagram demonstrates the relationship between the GVL release threshold value,
latency and throughput:

```
less latency & throughput <<< GVL release threshold >>> more latency & throughput
```

### Dealing with a Busy Database

When multiple threads or processes access the same database, the database may be
locked for writing by one process, which will block other processes wishing to
write to the database. When attempting to write to a locked database, a
`Extralite::BusyError` will be raised:

```ruby
ready = nil
locker = Thread.new do
  db1 = Extralite::Database.new('my.db')
  # Lock the database for 3 seconds
  db1.transaction do
    ready = true
    sleep(3)
  end
end

db2 = Extralite::Database.new('my.db')
# wait for writer1 to enter a transaction
sleep(0) while !ready
# This will raise a Extralite::BusyError
db2.transaction { }
# Extralite::BusyError!
```

You can mitigate this by setting a busy timeout. This will cause SQLite to wait
for the database to become unlocked up to the specified timeout period:

```ruby
# Wait for up to 5 seconds before giving up
db2.busy_timeout = 5
# Now it'll work!
db2.transaction { }
```

As stated above, setting the database to use WAL journaling mode greatly reduces
contention between different process/threads accessing the same database. For
most use cases, setting the busy timeout solves the problem of failing to run
queries because of a busy database, as normally transactions are short-lived.

However, in some cases, such as when running a multi-fibered app or when
implementing your own timeout mechanisms, you'll want to set a [progress
handler](#the-progress-handler).

### Interrupting a Query

To interrupt an ongoing query, use the `#interrupt` method. Normally this is
done from a separate thread. Here's a way to implement a timeout using
`#interrupt`:

```ruby
def run_query_with_timeout(sql, timeout)
  timeout_thread = Thread.new do
    t0 = Time.now
    sleep(timeout)
    @db.interrupt
  end
  result = @db.query(sql)
  timeout_thread.kill
  result
end

run_query_with_timeout('select 1 as foo', 5)
#=> [{ foo: 1 }]

# A timeout will cause a Extralite::InterruptError to be raised
run_query_with_timeout(slow_sql, 5)
#=> Extralite::InterruptError!
```

You can also call `#interrupt` from within the [progress
handler](#the-progress-handler).

### The Progress Handler

Extralite also supports setting up a progress handler, which is a piece of code
that will be called while a query is in progress, or while the database is busy.
This is useful especially when you want to implement a general purpose timeout
mechanism that deals with both a busy database and with slow queries.

The progress handler can also be used for performing any kind of operation while
a query is in progress. Here are some use cases:

- Interrupting queries that take too long to run.
- Interrupting queries on an exceptional condition, such as a received signal.
- Updating the UI while a query is running.
- Switching between fibers in multi-fibered apps.
- Switching between threads in multi-threaded apps.
- Instrumenting the performance of queries.

Setting the progress handler requires that Extralite hold the GVL while running
all queries. Therefore, it should be used with care. In a multi-threaded app,
you'll need to call `Thread.pass` from the progress handler in order for other
threads to be able to run while the query is in progress. The progress handler
is set per-database using `#on_progress`. This method takes a single parameter
that specifies the approximate number of SQLite VM instructions between
successive calls to the progress handler:

```ruby
db.on_progress do
  check_for_timeout
  # Allow other threads to run
  Thread.pass
end
```

The progress handler can be used to interrupt queries in progress. This can be
done by either calling `#interrupt`, or by raising an exception. As discussed
above, calling `#interrupt` causes the query to raise a
`Extralite::InterruptError` exception:

```ruby
db.on_progress(period: 1) { db.interrupt }
db.query('select 1')
#=> Extralite::InterruptError!
```

You can also interrupt queries in progress by raising an exception. The query
will be stopped, and the exception will propagate to the call site:

```ruby
db.on_progress(period: 1) do
  raise 'BOOM!'
end

db.query('select 1')
#=> BOOM!
```

Here's how a timeout might be implemented using the progress handler:

```ruby
def setup_progress_handler
  @db.on_progress do
    raise TimeoutError if Time.now - @t0 >= @timeout
    Thread.pass
  end
end

# In this example, we just return nil on timeout
def run_query_with_timeout(sql, timeout)
  @t0 = Time.now
  @db.query(sql)
rescue TimeoutError
  nil
end

run_query_with_timeout('select 1 as foo', 5)
#=> [{ foo: 1 }]

run_query_with_timeout(slow_sql, 5)
#=> nil
```

**Note**: you must not issue any query from within the progress handler.

### Dealing with a Busy Database in the Progress Handler

As mentioned above, the progress handler is also called when the database is
busy, regardless of the progress period given to `#on_progress`. You can detect
if the database is busy by checking the first argument passed to the progress
handler, which will be true when busy:

```ruby
db.on_progress do |busy|
  if busy
    foo
  else
    bar
  end
end
```

This allows you to implement separate logic to deal with busy states, for
example sleeping for a small period of time, or implementing a different timeout
period.

### Advanced Progress Handler Settings

You can further tune the behaviour of the progress handler with the following
options passed to `#on_progress`:

- `:mode`: the following modes are supported:
  - `:none` : the progress handler is disabled.
  - `:normal`: the progress handler is called on query progress (this is the
    default mode).
  - `:once`: the progress handler is called once before running the query.
  - `:at_least_once`: the progress handler is called once before running the
    query, and on query progress.
- `:period`: controls the approximate number of SQLite VM instructions executed
  between consecutive calls to the progress handler. Default value: 1000.
- `:tick`: controls the granularity of the progress handler. This is the value
  passed internally to the SQLite library. Default value: 10.

```ruby
db.on_progress(mode: :at_least_once, period: 640, tick: 5) { snooze }
```

### Global Progress Handler Settings

You can set the global progress handler behaviour by calling
`Extralite.on_progress`. You can use this API to set the global progress
settings, without needing to set a progress handler individually for each
`Database` instance. This method takes the same options as
`Database#on_progress`:

```ruby
Extralite.on_progress(mode: :at_least_once, period: 640, tick: 5) { snooze }

# the new database instance uses the global progress handler settings
db = Database.new(':memory:')
```

### Extralite and Fibers

The progress handler can also be used to switch between fibers in a
multi-fibered Ruby app, based on libraries such as
[Async](https://github.com/socketry/async) or
[Polyphony](https://github.com/digital-fabric/polyphony). A general solution
(that also works for multi-threaded apps) is to call `sleep(0)` in the progress
handler. This will work for switching between fibers using either Polyphony or
any fiber scheduler gem, such as Async et al:

```ruby
db.on_progress { sleep(0) }
```

For Polyphony-based apps, you can also call `snooze` to allow other fibers to
run while a query is progressing. If your Polyphony app is multi-threaded,
you'll also need to call `Thread.pass` in order to allow other threads to run:

```ruby
db.on_progress do
  snooze
  Thread.pass
end
```

Note that with Polyphony, once you install the progress handler, you can just
use the regular `#move_on_after` and `#cancel_after` methods to implement
timeouts for queries:

```ruby
db.on_progress { snooze }

cancel_after(3) do
  db.query(long_running_query)
end
```

### Thread Safety

A single database instance can be safely used in multiple threads simultaneously
as long as the following conditions are met:

- No explicit transactions are used.
- Each thread issues queries by calling `Database#query_xxx`, or uses a separate
  `Query` instance.
- The GVL release threshold is not `0` (i.e. the GVL is released periodically
  while running queries.)

### Use with Ractors

Extralite databases can safely be used inside ractors. A ractor has the benefit
of using a separate GVL from the maine one, which allows true parallelism for
Ruby apps. So when you use Extralite to access SQLite databases from within a
ractor, you can do so without any considerations for what's happening outside
the ractor when it runs queries.

**Note**: Ractors are considered an experimental feature of Ruby. You may
encounter errors or inconsistent behaviour when using ractors.

## Advanced Usage

### Loading Extensions

Extensions can be loaded by calling `#load_extension`:

```ruby
db.load_extension('/path/to/extension.so')
```

A pretty comprehensive set of extensions can be found here:

https://github.com/nalgeon/sqlean

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

### Working with Changesets

__Note__: as the session extension is by default disabled in SQLite
distributions, support for changesets is currently only available withthe
bundled version of Extralite, `extralite-bundle`.

Changesets can be used to track and persist changes to data in a database. They
can also be used to apply the same changes to another database, or to undo them.
To track changes to a database, use the `#track_changes` method:

```ruby
# track changes to the foo and bar tables:
changeset = db.track_changes(:foo, :bar) do
  insert_a_bunch_of_records(db)
end

# to track changes to all tables, pass nil:
changeset = db.track_changes(nil) do
  insert_a_bunch_of_records(db)
end
```

You can then apply the same changes to another database:

```ruby
changeset.apply(some_other_db)
```

To undo the changes, obtain an inverted changeset and apply it to the database:

```ruby
changeset.invert.apply(db)
```

You can also save and load the changeset:

```ruby
# save the changeset
IO.write('my.changes', changeset.to_blob)

# load the changeset
changeset = Extralite::Changeset.new
changeset.load(IO.read('my.changes'))
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

### Tracing SQL Statements

To trace all SQL statements executed on the database, pass a block to
`Database#trace`. To disable tracing, call `Database#trace` without a block:

```ruby
# enable tracing
db.trace { |sql| p sql: sql }

# disable tracing
db.trace
```

Any bound parameters will also be passed to the trace block:

```ruby
db.trace { |sql, *args| p sql: sql, args: args }
db.query('select ?, ?', 1, 2)
# { sql: "select ?, ?", args: [1, 2] }
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

## Performance

A benchmark script is included, creating a table of various row counts, then
fetching the entire table using either `sqlite3` or `extralite`. This benchmark
shows Extralite to be up to ~4.5 times faster than `sqlite3` when fetching a
large number of rows.

### Rows as Hashes

[Benchmark source
code](https://github.com/digital-fabric/extralite/blob/main/test/perf_hash.rb)

|Row count|sqlite3 2.6.0|Extralite 2.12|Advantage|
|-:|-:|-:|-:|
|10|629.0K rows/s|950.4K rows/s|__1.51x__|
|1K|1770.5K rows/s|4321.5K rows/s|__2.44x__|
|100K|1028.8K rows/s|4088.7K rows/s|__3.97x__|

### Rows as Arrays

[Benchmark source
code](https://github.com/digital-fabric/extralite/blob/main/test/perf_array.rb)

|Row count|sqlite3 2.6.0|Extralite 2.12|Advantage|
|-:|-:|-:|-:|
|10|889.4K rows/s|1000.1K rows/s|__1.13x__|
|1K|4518.1K rows/s|5381.5K rows/s|__1.19x__|
|100K|4454.0K rows/s|5083.8K rows/s|__1.14x__|

### Prepared Queries (Prepared Statements)

[Benchmark source
code](https://github.com/digital-fabric/extralite/blob/main/test/perf_hash_prepared.rb)

|Row count|sqlite3 2.6.0|Extralite 2.12|Advantage|
|-:|-:|-:|-:|
|10|783.1K rows/s|1115.1K rows/s|__1.42x__|
|1K|1782.5K rows/s|4635.5K rows/s|__2.60x__|
|100K|1018.1K rows/s|4599.4K rows/s|__4.52x__|

As those benchmarks show, Extralite is capabale of reading up to 4.5M rows per
second, and can be more than 4 times faster than the `sqlite3` gem.

Note that the benchmarks above were performed on synthetic data, in a
single-threaded environment, with the GVL release threshold set to -1, which
means that both Extralite and the `sqlite3` gem hold the GVL for the duration of
the query.

## License

The source code for Extralite is published under the [MIT license](LICENSE). The
source code for SQLite is in the [public
domain](https://sqlite.org/copyright.html).

## Contributing

Contributions in the form of issues, PRs or comments will be greatly
appreciated!
