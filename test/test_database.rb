# frozen_string_literal: true

require_relative 'helper'

require 'date'
require 'tempfile'
require 'json'

class DatabaseTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.query('create table if not exists t (x,y,z)')
    @db.query('delete from t')
    @db.query('insert into t values (1, 2, 3)')
    @db.query('insert into t values (4, 5, 6)')
  end

  def test_database
    r = @db.query('select 1 as foo')
    assert_equal [{foo: 1}], r
  end

  def test_query
    r = @db.query('select * from t')
    assert_equal [{x: 1, y: 2, z: 3}, {x: 4, y: 5, z: 6}], r

    r = @db.query('select * from t where x = 2')
    assert_equal [], r
  end

  def test_invalid_query
    assert_raises(Extralite::SQLError) { @db.query('blah') }
  end

  def test_query_hash
    r = @db.query_hash('select * from t')
    assert_equal [{x: 1, y: 2, z: 3}, {x: 4, y: 5, z: 6}], r

    r = @db.query_hash('select * from t where x = 2')
    assert_equal [], r
  end

  def test_query_hash_with_many_columns
    # this tests correct processing cof column names when column count is more than
    # MAX_EMBEDDED_COLUMN_NAMES
    r = @db.query_hash("
      select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e, 6 as f, 7 as g, 8 as h, 9 as i, 10 as j,
      11 as k, 12 as l, 13 as m, 14 as n, 15 as o, 16 as p, 17 as q, 18 as r, 19 as s, 20 as t
    ")
    assert_equal [{
      a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8, i: 9, j: 10,
      k: 11, l: 12, m: 13, n: 14, o: 15, p: 16, q: 17, r: 18, s: 19, t: 20
    }], r
  end

  def test_query_array
    r = @db.query_array('select * from t')
    assert_equal [[1, 2, 3], [4, 5, 6]], r

    r = @db.query_array('select * from t where x = 2')
    assert_equal [], r
  end

  def test_query_splat
    r = @db.query_splat('select * from t')
    assert_equal [[1, 2, 3], [4, 5, 6]], r

    r = @db.query_splat('select * from t where x = 2')
    assert_equal [], r

    # with block
    r = []
    @db.query_splat('select * from t') { |a, b, c| r << [a, b, c] }
    assert_equal [[1, 2, 3], [4, 5, 6]], r
  end

  def test_query_splat_with_too_many_columns
    assert_raises(Extralite::Error) {
      @db.query_splat('select 1, 2, 3, 4, 5, 6, 7, 8, 9');
    }
  end

  def test_query_single
    r = @db.query_single('select * from t order by x desc limit 1')
    assert_equal({ x: 4, y: 5, z: 6 }, r)

    r = @db.query_single('select * from t where x = 2')
    assert_nil r
  end

  def test_query_single_splat
    r = @db.query_single_splat('select z from t order by Z desc limit 1')
    assert_equal 6, r

    r = @db.query_single_splat('select z from t where x = 2')
    assert_nil r
  end

  def test_columns
    r = @db.columns('select x, z from t')
    assert_equal [:x, :z], r
  end

  def test_transaction_active?
    assert_equal false, @db.transaction_active?
    @db.query('begin')
    assert_equal true, @db.transaction_active?
    @db.query('rollback')
    assert_equal false, @db.transaction_active?
  end

  def test_multiple_statements
    @db.query("insert into t values ('a', 'b', 'c'); insert into t values ('d', 'e', 'f');")

    assert_equal [1, 4, 'a', 'd'], @db.query_splat('select x from t order by x')
  end

  def test_multiple_statements_with_error
    error = nil
    begin
      @db.query("insert into t values foo; insert into t values ('d', 'e', 'f');")
    rescue => error
    end

    assert_kind_of Extralite::SQLError, error
    assert_equal 'near "foo": syntax error', error.message
  end

  def test_empty_sql
    r = @db.query(' ')
    assert_nil r

    r = @db.query('select 1 as foo;  ')
    assert_equal [{ foo: 1 }], r
  end

  def test_close
    assert_equal false, @db.closed?
    r = @db.query_single_splat('select 42')
    assert_equal 42, r

    assert_equal @db, @db.close
    assert_equal true, @db.closed?

    assert_raises(Extralite::Error) { @db.query_single_splat('select 42') }
  end

  def test_parameter_binding_simple
    r = @db.query('select x, y, z from t where x = ?', 1)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.query('select x, y, z from t where z = ?', 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r

    error = assert_raises(Extralite::ParameterError) { @db.query_single_splat('select ?', Date.today) }
    assert_equal error.message, 'Cannot bind parameter at position 1 of type Date'
  end

  def test_parameter_binding_with_index
    r = @db.query('select x, y, z from t where x = ?2', 0, 1)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.query('select x, y, z from t where z = ?3', 3, 4, 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_parameter_binding_with_name
    r = @db.query('select x, y, z from t where x = :x', x: 1, y: 2)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.query('select x, y, z from t where z = :zzz', 'zzz' => 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r

    r = @db.query('select x, y, z from t where z = :bazzz', ':bazzz' => 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_parameter_binding_with_index_key
    r = @db.query('select x, y, z from t where z = ?', 1 => 3)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.query('select x, y, z from t where x = ?2', 1 => 42, 2 => 4)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  class Foo; end

  def test_parameter_binding_from_hash
    assert_equal 42, @db.query_single_splat('select :bar', foo: 41, bar: 42)
    assert_equal 42, @db.query_single_splat('select :bar', 'foo' => 41, 'bar' => 42)
    assert_equal 42, @db.query_single_splat('select ?8', 7 => 41, 8 => 42)
    assert_nil @db.query_single_splat('select :bar', foo: 41)

    error = assert_raises(Extralite::ParameterError) { @db.query_single_splat('select ?', Foo.new => 42) }
    assert_equal error.message, 'Cannot bind parameter with a key of type DatabaseTest::Foo'

    error = assert_raises(Extralite::ParameterError) { @db.query_single_splat('select ?', %w[a b] => 42) }
    assert_equal error.message, 'Cannot bind parameter with a key of type Array'
  end

  def test_parameter_binding_from_struct
    foo_bar = Struct.new(:':foo', :bar)
    value = foo_bar.new(41, 42)
    assert_equal 41, @db.query_single_splat('select :foo', value)
    assert_equal 42, @db.query_single_splat('select :bar', value)
    assert_nil @db.query_single_splat('select :baz', value)
  end

  def test_parameter_binding_from_data_class
    skip "Data isn't supported in Ruby < 3.2" if RUBY_VERSION < '3.2'

    foo_bar = Data.define(:':foo', :bar)
    value = foo_bar.new(':foo': 41, bar: 42)
    assert_equal 42, @db.query_single_splat('select :bar', value)
    assert_nil @db.query_single_splat('select :baz', value)
  end

  def test_parameter_binding_for_blobs
    sql = 'SELECT typeof(data) AS type, data FROM blobs WHERE ROWID = ?'
    blob_path = File.expand_path('fixtures/image.png', __dir__)
    @db.execute('CREATE TABLE blobs (data BLOB)')

    # it's a string, not a blob
    @db.execute('INSERT INTO blobs VALUES (?)', 'Hello, 世界!')
    result = @db.query_single(sql, @db.last_insert_rowid)
    assert_equal 'text', result[:type]
    assert_equal Encoding::UTF_8, result[:data].encoding

    data = File.binread(blob_path)
    @db.execute('INSERT INTO blobs VALUES (?)', data)
    result = @db.query_single(sql, @db.last_insert_rowid)
    assert_equal 'blob', result[:type]
    assert_equal data, result[:data]

    data = (+'Hello, 世界!').force_encoding(Encoding::ASCII_8BIT)
    @db.execute('INSERT INTO blobs VALUES (?)', data)
    result = @db.query_single(sql, @db.last_insert_rowid)
    assert_equal 'blob', result[:type]
    assert_equal Encoding::ASCII_8BIT, result[:data].encoding
    assert_equal 'Hello, 世界!', result[:data].force_encoding(Encoding::UTF_8)

    data = Extralite::Blob.new('Hello, 世界!')
    @db.execute('INSERT INTO blobs VALUES (?)', data)
    result = @db.query_single(sql, @db.last_insert_rowid)
    assert_equal 'blob', result[:type]
    assert_equal Encoding::ASCII_8BIT, result[:data].encoding
    assert_equal 'Hello, 世界!', result[:data].force_encoding(Encoding::UTF_8)
  end

  def test_parameter_binding_for_simple_types
    assert_nil @db.query_single_splat('select ?', nil)

    # 32-bit integers
    assert_equal(-2** 31, @db.query_single_splat('select ?', -2**31))
    assert_equal(2**31 - 1, @db.query_single_splat('select ?', 2**31 - 1))

    # 64-bit integers
    assert_equal(-2 ** 63, @db.query_single_splat('select ?', -2 ** 63))
    assert_equal(2**63 - 1, @db.query_single_splat('select ?', 2**63 - 1))

    # floats
    assert_equal Float::MIN, @db.query_single_splat('select ?', Float::MIN)
    assert_equal Float::MAX, @db.query_single_splat('select ?', Float::MAX)

    # boolean
    assert_equal 1, @db.query_single_splat('select ?', true)
    assert_equal 0, @db.query_single_splat('select ?', false)

    # strings and symbols
    assert_equal 'foo', @db.query_single_splat('select ?', 'foo')
    assert_equal 'foo', @db.query_single_splat('select ?', :foo)
  end

  def test_value_casting
    r = @db.query_single_splat("select 'abc'")
    assert_equal 'abc', r

    r = @db.query_single_splat('select 123')
    assert_equal 123, r

    r = @db.query_single_splat('select 12.34')
    assert_equal 12.34, r

    r = @db.query_single_splat('select zeroblob(4)')
    assert_equal "\x00\x00\x00\x00", r

    r = @db.query_single_splat('select null')
    assert_nil r
  end

  def test_extension_loading
    case RUBY_PLATFORM
    when /aarch64-linux/
      @db.load_extension(File.join(__dir__, 'extensions/arm64/text.so'))
    when /x86_64-linux/
      @db.load_extension(File.join(__dir__, 'extensions/x86/text.so'))
    when /arm64-darwin/
      @db.load_extension(File.join(__dir__, 'extensions/arm64/text.dylib'))
    when /x86_64-darwin/
      @db.load_extension(File.join(__dir__, 'extensions/x86/text.dylib'))
    end

    r = @db.query_single_splat("select reverse('abcd')")
    assert_equal 'dcba', r
  end

  def test_tables
    assert_equal ['t'], @db.tables

    @db.query('create table foo (bar text)')
    assert_equal ['t', 'foo'], @db.tables

    @db.query('drop table t')
    assert_equal ['foo'], @db.tables

    @db.query('drop table foo')
    assert_equal [], @db.tables
  end

  def test_tables_with_db_name
    assert_equal ['t'], @db.tables('main')

    @db.query('create table foo (bar text)')
    assert_equal ['t', 'foo'], @db.tables('main')

    @db.query('drop table t')
    assert_equal ['foo'], @db.tables('main')

    @db.query('drop table foo')
    assert_equal [], @db.tables('main')

    assert_raises { @db.tables('foo') }

    fn = Tempfile.new('extralite_test_tables_with_db_name').path
    @db.execute "attach database '#{fn}' as foo"

    assert_equal [], @db.tables('foo')
    @db.execute 'create table foo.bar (x, y)'
    assert_equal ['bar'], @db.tables('foo')
  end

  def test_pragma
    assert_equal 'memory', @db.pragma('journal_mode')
    assert_equal 2, @db.pragma('synchronous')

    assert_equal 1, @db.pragma(:schema_version)
    assert_equal 0, @db.pragma(:recursive_triggers)

    assert_equal [], @db.pragma(schema_version: 33, recursive_triggers: 1)
    assert_equal 33, @db.pragma(:schema_version)
    assert_equal 1, @db.pragma(:recursive_triggers)
  end

  def test_execute
    changes = @db.execute('update t set x = 42')
    assert_equal 2, changes
  end

  def test_execute_with_params
    changes = @db.execute('update t set x = ? where z = ?', 42, 6)
    assert_equal 1, changes
    assert_equal [[1, 2, 3], [42, 5, 6]], @db.query_array('select * from t order by x')
  end

  def test_execute_with_params_array
    changes = @db.execute('update t set x = ? where z = ?', [42, 6])
    assert_equal 1, changes
    assert_equal [[1, 2, 3], [42, 5, 6]], @db.query_array('select * from t order by x')
  end

  def test_batch_execute
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    records = [
      [1, '2', 3],
      ['4', 5, 6]
    ]
    changes = @db.batch_execute('insert into foo values (?, ?, ?)', records)

    assert_equal 2, changes
    assert_equal [
      { a: 1, b: '2', c: 3 },
      { a: '4', b: 5, c: 6 }
    ], @db.query('select * from foo')
  end

  def test_batch_execute_with_array_of_hashes
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    records = [
      { a: 1, b: '2', c: 3 },
      { a: '4', b: 5, c: 6 }
    ]
    changes = @db.batch_execute('insert into foo values (:a, :b, :c)', records)

    # assert_equal 2, changes
    assert_equal [
      { a: 1, b: '2', c: 3 },
      { a: '4', b: 5, c: 6 }
    ], @db.query('select * from foo')
  end

  def test_batch_execute_single_values
    @db.query('create table foo (bar)')
    assert_equal [], @db.query('select * from foo')

    records = [
      'hi',
      'bye'
    ]

    changes = @db.batch_execute('insert into foo values (?)', records)

    assert_equal 2, changes
    assert_equal [
      { bar: 'hi' },
      { bar: 'bye' }
    ], @db.query('select * from foo')
  end

  def test_batch_execute_with_each_interface
    @db.query('create table foo (bar)')
    assert_equal [], @db.query('select * from foo')

    changes = @db.batch_execute('insert into foo values (?)', 1..3)

    assert_equal 3, changes
    assert_equal [
      { bar: 1 },
      { bar: 2 },
      { bar: 3 }
    ], @db.query('select * from foo')
  end

  def test_batch_execute_with_proc
    source = [42, 43, 44]

    @db.query('create table foo (a)')
    assert_equal [], @db.query('select * from foo')

    pr = proc { source.shift }
    changes = @db.batch_execute('insert into foo values (?)', pr)

    assert_equal 3, changes
    assert_equal [
      { a: 42 },
      { a: 43 },
      { a: 44 }
    ], @db.query('select * from foo')
  end

  def test_batch_query_with_array
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    data = [
      [1, '2', 3],
      ['4', 5, 6]
    ]
    results = @db.batch_query('insert into foo values (?, ?, ?) returning *', data)
    assert_equal [
      [{ a: 1, b: '2', c: 3 }],
      [{ a: '4', b: 5, c: 6 }]
    ], results

    results = @db.batch_query('update foo set c = ? returning *', [42, 43])
    assert_equal [
      [{ a: 1, b: '2', c: 42 }, { a: '4', b: 5, c: 42 }],
      [{ a: 1, b: '2', c: 43 }, { a: '4', b: 5, c: 43 }]
    ], results

    array = []
    changes = @db.batch_query('update foo set c = ? returning *', [44, 45]) do |rows|
      array << rows
    end
    assert_equal 4, changes
    assert_equal [
      [{ a: 1, b: '2', c: 44 }, { a: '4', b: 5, c: 44 }],
      [{ a: 1, b: '2', c: 45 }, { a: '4', b: 5, c: 45 }]
    ], array
  end

  def test_batch_query_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    results = @db.batch_query('insert into foo (b) values (?) returning *', 11..13)
    assert_equal [
      [{ a: 1, b: 11 }],
      [{ a: 2, b: 12 }],
      [{ a: 3, b: 13 }]
    ], results

    results = @db.batch_query('update foo set b = ? returning *', [42, 43])
    assert_equal [
      [{ a: 1, b: 42 }, { a: 2, b: 42 }, { a: 3, b: 42 }],
      [{ a: 1, b: 43 }, { a: 2, b: 43 }, { a: 3, b: 43 }]
    ], results

    array = []
    changes = @db.batch_query('update foo set b = ? returning *', [44, 45]) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [{ a: 1, b: 44 }, { a: 2, b: 44 }, { a: 3, b: 44 }],
      [{ a: 1, b: 45 }, { a: 2, b: 45 }, { a: 3, b: 45 }]
    ], array
  end

  def parameter_source_proc(values)
    proc { values.shift }
  end

  def test_batch_query_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    pr = parameter_source_proc([5, 4, 3])
    assert_kind_of Proc, pr
    results = @db.batch_query('insert into foo (b) values (?) returning *', pr)
    assert_equal [
      [{ a: 1, b: 5 }],
      [{ a: 2, b: 4 }],
      [{ a: 3, b: 3 }]
    ], results

    pr = parameter_source_proc([42, 43])
    results = @db.batch_query('update foo set b = ? returning *', pr)
    assert_equal [
      [{ a: 1, b: 42 }, { a: 2, b: 42 }, { a: 3, b: 42 }],
      [{ a: 1, b: 43 }, { a: 2, b: 43 }, { a: 3, b: 43 }]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = @db.batch_query('update foo set b = ? returning *', pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [{ a: 1, b: 44 }, { a: 2, b: 44 }, { a: 3, b: 44 }],
      [{ a: 1, b: 45 }, { a: 2, b: 45 }, { a: 3, b: 45 }]
    ], array
  end

  def test_batch_query_array_with_array
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    data = [
      [1, '2', 3],
      ['4', 5, 6]
    ]
    results = @db.batch_query_array('insert into foo values (?, ?, ?) returning *', data)
    assert_equal [
      [[1, '2', 3]],
      [['4', 5, 6]]
    ], results

    results = @db.batch_query_array('update foo set c = ? returning *', [42, 43])
    assert_equal [
      [[1, '2', 42], ['4', 5, 42]],
      [[1, '2', 43], ['4', 5, 43]]
    ], results

    array = []
    changes = @db.batch_query_array('update foo set c = ? returning *', [44, 45]) do |rows|
      array << rows
    end
    assert_equal 4, changes
    assert_equal [
      [[1, '2', 44], ['4', 5, 44]],
      [[1, '2', 45], ['4', 5, 45]]
    ], array
  end

  def test_batch_query_array_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    results = @db.batch_query_array('insert into foo (b) values (?) returning *', 11..13)
    assert_equal [
      [[1, 11]],
      [[2, 12]],
      [[3, 13]]
    ], results

    results = @db.batch_query_array('update foo set b = ? returning *', [42, 43])
    assert_equal [
      [[1, 42], [2, 42], [3, 42]],
      [[1, 43], [2, 43], [3, 43]]
    ], results

    array = []
    changes = @db.batch_query_array('update foo set b = ? returning *', [44, 45]) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [[1, 44], [2, 44], [3, 44]],
      [[1, 45], [2, 45], [3, 45]]
    ], array
  end

  def test_batch_query_array_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    pr = parameter_source_proc([5, 4, 3])
    assert_kind_of Proc, pr
    results = @db.batch_query_array('insert into foo (b) values (?) returning *', pr)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    pr = parameter_source_proc([42, 43])
    results = @db.batch_query_array('update foo set b = ? returning *', pr)
    assert_equal [
      [[1, 42], [2, 42], [3, 42]],
      [[1, 43], [2, 43], [3, 43]]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = @db.batch_query_array('update foo set b = ? returning *', pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [[1, 44], [2, 44], [3, 44]],
      [[1, 45], [2, 45], [3, 45]]
    ], array
  end

  def test_batch_query_splat_with_array
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    data = [
      [1, '2', 3],
      ['4', 5, 6]
    ]
    results = @db.batch_query_splat('insert into foo values (?, ?, ?) returning c', data)
    assert_equal [
      [3],
      [6]
    ], results

    results = @db.batch_query_splat('update foo set c = ? returning c * 10 + cast(b as integer)', [42, 43])
    assert_equal [
      [422, 425],
      [432, 435]
    ], results

    array = []
    changes = @db.batch_query_splat('update foo set c = ? returning c * 10 + cast(b as integer)', [44, 45]) do |rows|
      array << rows
    end
    assert_equal 4, changes
    assert_equal [
      [442, 445],
      [452, 455]
    ], array
  end

  def test_batch_query_splat_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    results = @db.batch_query_splat('insert into foo (b) values (?) returning b * 10 + a', 11..13)
    assert_equal [
      [111],
      [122],
      [133]
    ], results

    results = @db.batch_query_splat('update foo set b = ? returning b * 10 + a', 42..43)
    assert_equal [
      [421, 422, 423],
      [431, 432, 433]
    ], results

    array = []
    changes = @db.batch_query_splat('update foo set b = ? returning b * 10 + a', 44..45) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [441, 442, 443],
      [451, 452, 453]
    ], array
  end

  def test_batch_query_splat_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    pr = parameter_source_proc([5, 4, 3])
    assert_kind_of Proc, pr
    results = @db.batch_query_splat('insert into foo (b) values (?) returning b', pr)
    assert_equal [
      [5],
      [4],
      [3]
    ], results

    pr = parameter_source_proc([42, 43])
    results = @db.batch_query_splat('update foo set b = ? returning b * 10 + a', pr)
    assert_equal [
      [421, 422, 423],
      [431, 432, 433]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = @db.batch_query_splat('update foo set b = ? returning b * 10 + a', pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [441, 442, 443],
      [451, 452, 453]
    ], array
  end

  def test_interrupt
    t = Thread.new do
      sleep 0.5
      @db.interrupt
    end

    n = 2**31
    assert_raises(Extralite::InterruptError) {
      @db.query <<-SQL
        WITH RECURSIVE
          fibo (curr, next)
        AS
        ( SELECT 1,1
          UNION ALL
          SELECT next, curr+next FROM fibo
          LIMIT #{n} )
        SELECT curr, next FROM fibo LIMIT 1 OFFSET #{n}-1;
      SQL
    }
    t.join
  ensure
    t&.kill
  end

  def test_database_status
    assert_operator 0, :<, @db.status(Extralite::SQLITE_DBSTATUS_SCHEMA_USED).first
  end

  def test_database_limit
    result = @db.limit(Extralite::SQLITE_LIMIT_ATTACHED)
    assert_equal 10, result

    result = @db.limit(Extralite::SQLITE_LIMIT_ATTACHED, 5)
    assert_equal 10, result

    result = @db.limit(Extralite::SQLITE_LIMIT_ATTACHED)
    assert_equal 5, result

    assert_raises(Extralite::Error) { @db.limit(-999) }
  end

  def test_database_busy_timeout
    fn = Tempfile.new('extralite_test_database_busy_timeout').path
    db1 = Extralite::Database.new(fn)
    db2 = Extralite::Database.new(fn)

    db1.query('begin exclusive')
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }

    db2.busy_timeout = 3
    t0 = Time.now
    t = Thread.new { sleep 0.1; db1.query('rollback') }
    result = db2.query('begin exclusive')
    t1 = Time.now

    assert_equal [], result
    assert t1 - t0 >= 0.1
    db2.query('rollback')
    t.join

    # try to provoke a timeout
    db1.query('begin exclusive')
    db2.busy_timeout = nil
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }

    db2.busy_timeout = 0.2
    t0 = Time.now
    t = Thread.new do
      sleep 3
    ensure
      db1.query('rollback')
    end
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }

    t1 = Time.now
    assert t1 - t0 >= 0.2
    t.kill
    t.join

    db1.query('begin exclusive')
    db2.busy_timeout = 0
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }

    db2.busy_timeout = nil
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }
  ensure
    t&.kill
  end

  def test_database_total_changes
    assert_equal 2, @db.total_changes

    @db.query('insert into t values (7, 8, 9)')

    assert_equal 3, @db.total_changes
  end

  def test_database_errcode_errmsg
    assert_equal 0, @db.errcode
    assert_equal 'not an error', @db.errmsg

    @db.query('select foo') rescue nil

    assert_equal 1, @db.errcode
    assert_equal 'no such column: foo', @db.errmsg

    if Extralite.sqlite3_version >= '3.38.5'
      assert_equal 7, @db.error_offset
    end

    @db.query('create table t2 (v not null)')

    assert_raises(Extralite::Error) { @db.query('insert into t2 values (null)') }
    assert_equal Extralite::SQLITE_CONSTRAINT_NOTNULL, @db.errcode
    assert_equal 'NOT NULL constraint failed: t2.v', @db.errmsg
  end


  def test_close_with_open_prepared_statement
    query = @db.prepare('select * from t')
    query.next
    @db.close
  end

  def test_read_only_database
    db = Extralite::Database.new(':memory:')
    db.query('create table foo (bar)')
    assert_equal false, db.read_only?

    db = Extralite::Database.new(':memory:', read_only: true)
    assert_raises(Extralite::Error) { db.query('create table foo (bar)') }
    assert_equal true, db.read_only?
  end

  def test_database_initialize_options
    db = Extralite::Database.new(':memory:', gvl_release_threshold: 23)
    assert_equal 23, db.gvl_release_threshold

    fn = Tempfile.new('extralite_test_database_initialize_options_wal').path
    db = Extralite::Database.new(fn, wal: true)
    assert_equal 'wal', db.pragma(:journal_mode)
    assert_equal 1, db.pragma(:synchronous)

    fn = Tempfile.new('extralite_test_database_initialize_options_pragma').path
    db = Extralite::Database.new(fn, pragma: { application_id: 42 })
    assert_equal 42, db.pragma(:application_id)
  end

  def test_database_inspect
    db = Extralite::Database.new(':memory:')
    assert_match(/^\#\<Extralite::Database:0x[0-9a-f]+ :memory:\>$/, db.inspect)
  end

  def test_database_inspect_on_closed_database
    db = Extralite::Database.new(':memory:')
    assert_match(/^\#\<Extralite::Database:0x[0-9a-f]+ :memory:\>$/, db.inspect)
    db.close
    assert_match(/^\#\<Extralite::Database:0x[0-9a-f]+ \(closed\)\>$/, db.inspect)
  end

  def test_string_encoding
    db = Extralite::Database.new(':memory:')
    v = db.query_single_splat("select 'foo'")
    assert_equal 'foo', v
    assert_equal 'UTF-8', v.encoding.name
  end

  def test_database_transaction_commit
    path = Tempfile.new('extralite_test_database_transaction_commit').path
    db1 = Extralite::Database.new(path)
    db2 = Extralite::Database.new(path)

    db1.execute('create table foo(x)')
    assert_equal ['foo'], db1.tables
    assert_equal ['foo'], db2.tables

    q1 = Queue.new
    q2 = Queue.new
    t = Thread.new do
      db1.transaction do
        assert_equal true, db1.transaction_active?
        db1.execute('insert into foo values (42)')
        q1 << true
        q2.pop
      end
      assert_equal false, db1.transaction_active?
    end
    q1.pop
    # transaction not yet committed
    assert_equal false, db2.transaction_active?
    assert_equal [], db2.query('select * from foo')

    q2 << true
    t.join
    # transaction now committed
    assert_equal [{ x: 42 }], db2.query('select * from foo')
  ensure
    t&.kill
  end

  def test_database_transaction_rollback
    db = Extralite::Database.new(':memory:')
    db.execute('create table foo(x)')

    assert_equal [], db.query('select * from foo')

    exception = nil
    begin
      db.transaction do
        db.execute('insert into foo values (42)')
        raise 'bar'
      end
    rescue => e
      exception = e
    end

    assert_equal [], db.query('select * from foo')
    assert_kind_of RuntimeError, exception
    assert_equal 'bar', exception.message
  end

  def test_database_transaction_rollback!
    db = Extralite::Database.new(':memory:')
    db.execute('create table foo(x)')

    exception = nil
    begin
      db.transaction do
        db.execute('insert into foo values (42)')
        db.rollback!
      end
    rescue => e
      exception = e
    end

    assert_equal [], db.query('select * from foo')
    assert_nil exception
  end

  def test_database_savepoint
    db = Extralite::Database.new(':memory:')
    db.execute('create table foo(x)')

    db.transaction do
      assert_equal [], db.query('select * from foo')

      db.execute('insert into foo values (42)')
      assert_equal [42], db.query_splat('select x from foo')

      db.savepoint(:a)

      db.execute('insert into foo values (43)')
      assert_equal [42, 43], db.query_splat('select x from foo')

      db.savepoint(:b)

      db.execute('insert into foo values (44)')
      assert_equal [42, 43, 44], db.query_splat('select x from foo')

      db.rollback_to(:b)
      assert_equal [42, 43], db.query_splat('select x from foo')

      db.release(:a)

      assert_equal [42, 43], db.query_splat('select x from foo')
    end
  end

  def test_prepare
    q = @db.prepare('select * from t order by x')
    assert_kind_of Extralite::Query, q

    assert_equal [
      { x: 1, y: 2, z: 3},
      { x: 4, y: 5, z: 6}
    ], q.to_a
  end

  def test_prepare_splat
    q = @db.prepare_splat('select * from t order by x')
    assert_kind_of Extralite::Query, q

    buf = []
    q.each { |x, y, z| buf << z }
    assert_equal [3, 6], buf
  end

  def test_prepare_array
    q = @db.prepare_array('select * from t order by x')
    assert_kind_of Extralite::Query, q

    assert_equal [
      [1, 2, 3],
      [4, 5, 6]
    ], q.to_a
  end

  def test_prepare_with_transform
    q = @db.prepare('select * from t order by x') { |h| h[:x] * 100 + h[:y] * 10 + h[:z] }
    assert_equal [
      123,
      456
    ], q.to_a
  end

  def test_prepare_splat_with_transform
    q = @db.prepare_splat('select * from t order by x') { |x, y, z| x * 100 + y * 10 + z }
    assert_equal [
      123,
      456
    ], q.to_a
  end

  def test_prepare_array_with_transform
    q = @db.prepare_array('select * from t order by x') { |r| r * 2 }
    assert_equal [
      [1, 2, 3, 1, 2, 3],
      [4, 5, 6, 4, 5, 6]
    ], q.to_a
  end

  def test_wal_checkpoint
    fn = Tempfile.new('extralite_test_wal_checkpoint').path

    db = Extralite::Database.new(fn, wal: true)

    db.execute 'create table t (x, y, z)'
    rows = (1..1000).map { [rand, rand, rand] }
    db.batch_execute('insert into t values (?, ?, ?)', rows)

    wal_fn = "#{fn}-wal"

    assert File.exist?(wal_fn)
    assert File.size(wal_fn) > 0

    # invalid mode
    assert_raises(ArgumentError) { db.wal_checkpoint(:foo) }

    # invalid db name
    assert_raises(Extralite::Error) { db.wal_checkpoint(:passive, 'foo') }

    r = db.wal_checkpoint(:full)
    assert File.exist?(wal_fn)
    assert File.size(wal_fn) > 0
    assert_equal [19, 19], r

    r = db.wal_checkpoint(:truncate)
    assert File.exist?(wal_fn)
    assert File.size(wal_fn) == 0
    assert_equal [0, 0], r
  end

  def test_execute_with_comments
    result = @db.execute(<<~SQL)
      -- this is a comment
    SQL
    assert_nil result

    result = @db.execute(<<~SQL)
      DELETE FROM t;
      INSERT INTO t (x, y, z) VALUES (1, 1, 1);
      INSERT INTO t (x, y, z) VALUES (2, 2, 2);
    SQL
    assert_equal 1, result
    assert_equal [1, 2], @db.query_splat('SELECT x FROM t ORDER BY x')

    result = @db.execute(<<~SQL)
      -- this is a comment at the beginning
      DELETE FROM t;
      INSERT INTO t (x, y, z) VALUES (3, 3, 3);
      INSERT INTO t (x, y, z) VALUES (4, 4, 4);
    SQL
    assert_equal 1, result
    assert_equal [3, 4], @db.query_splat('SELECT x FROM t ORDER BY x')

    result = @db.execute(<<~SQL)
      DELETE FROM t;
      INSERT INTO t (x, y, z) VALUES (5, 5, 5);
      -- this is a comment in the middle
      INSERT INTO t (x, y, z) VALUES (6, 6, 6);
    SQL
    assert_equal 1, result
    assert_equal [5, 6], @db.query_splat('SELECT x FROM t ORDER BY x')

    result = @db.execute(<<~SQL)
      DELETE FROM t;
      INSERT INTO t (x, y, z) VALUES (7, 7, 7);
      INSERT INTO t (x, y, z) VALUES (8, 8, 8);
      -- this is a comment at the end
    SQL
    assert_nil result
    assert_equal [7, 8], @db.query_splat('SELECT x FROM t ORDER BY x')
  end
end

class ScenarioTest < Minitest::Test
  def setup
    @fn = Tempfile.new('extralite_scenario_test').path
    @db = Extralite::Database.new(@fn)
    @db.query('create table if not exists t (x,y,z)')
    @db.query('delete from t')
    @db.query('insert into t values (1, 2, 3)')
    @db.query('insert into t values (4, 5, 6)')
  end

  def test_concurrent_transactions
    done = false
    t = Thread.new do
      db = Extralite::Database.new(@fn)
      db.query 'begin immediate'
      sleep 0.01 until done

      while true
        begin
          db.query 'commit'
          break
        rescue Extralite::BusyError
          sleep 0.01
        end
      end
    end

    sleep 0.1
    @db.query 'begin deferred'
    result = @db.query_splat('select x from t')
    assert_equal [1, 4], result

    assert_raises(Extralite::BusyError) do
      @db.query('insert into t values (7, 8, 9)')
    end

    done = true
    sleep 0.1

    assert_raises(Extralite::BusyError) do
      @db.query('insert into t values (7, 8, 9)')
    end

    assert_equal true, @db.transaction_active?

    # the thing to do in this case is to commit the read transaction, allowing
    # the other thread to commit its write transaction, and then we can
    # "upgrade" to a write transaction

    @db.query('commit')

    while true
      begin
        @db.query('begin immediate')
        break
      rescue Extralite::BusyError
        sleep 0.1
      end
    end

    @db.query('insert into t values (7, 8, 9)')
    @db.query('commit')

    result = @db.query_splat('select x from t')
    assert_equal [1, 4, 7], result
  ensure
    t&.kill
  end

  def test_concurrent_queries
    @db.query('delete from t')
    @db.gvl_release_threshold = 1
    q1 = @db.prepare('insert into t values (?, ?, ?)')
    q2 = @db.prepare('insert into t values (?, ?, ?)')

    t1 = Thread.new do
      data = (1..50).each_slice(10).map { |a| a.map { |i| [i, i + 1, i + 2] } }
      data.each do |params|
        q1.batch_execute(params)
      end
    end

    t2 = Thread.new do
      data = (51..100).each_slice(10).map { |a| a.map { |i| [i, i + 1, i + 2] } }
      data.each do |params|
        q2.batch_execute(params)
      end
    end

    t1.join
    t2.join

    assert_equal (1..100).to_a, @db.query_splat('select x from t order by x')
  ensure
    t1&.kill
    t2&.kill
  end

  def test_database_trace
    sqls = []
    @db.trace { |sql| sqls << sql }
    GC.start

    @db.query('select 1')
    assert_equal ['select 1'], sqls

    @db.query('select 2')
    assert_equal ['select 1', 'select 2'], sqls

    query = @db.prepare('select 3')

    query.to_a
    assert_equal ['select 1', 'select 2', 'select 3'], sqls

    # turn off
    @db.trace

    query.to_a

    @db.query('select 4')
    assert_equal ['select 1', 'select 2', 'select 3'], sqls
  end
end

class BackupTest < Minitest::Test
  def setup
    @src = Extralite::Database.new(':memory:')
    @dst = Extralite::Database.new(':memory:')

    @src.query('create table t (x,y,z)')
    @src.query('insert into t values (1, 2, 3)')
    @src.query('insert into t values (4, 5, 6)')
  end

  def test_backup
    @src.backup(@dst)
    assert_equal [[1, 2, 3], [4, 5, 6]], @dst.query_array('select * from t')
  end

  def test_backup_with_block
    progress = []
    @src.backup(@dst) { |r, t| progress << [r, t] }
    assert_equal [[1, 2, 3], [4, 5, 6]], @dst.query_array('select * from t')
    assert_equal [[2, 2]], progress
  end

  def test_backup_with_schema_names
    @src.backup(@dst, 'main', 'temp')
    assert_equal [[1, 2, 3], [4, 5, 6]], @dst.query_array('select * from temp.t')
  end

  def test_backup_with_fn
    tmp_fn = Tempfile.new('extralite_test_backup_with_fn').path
    @src.backup(tmp_fn)

    db = Extralite::Database.new(tmp_fn)
    assert_equal [[1, 2, 3], [4, 5, 6]], db.query_array('select * from t')
  end
end

class ConcurrencyTest < Minitest::Test
  def setup
    @sql = <<~SQL
      WITH RECURSIVE r(i) AS (
        VALUES(0)
        UNION ALL
        SELECT i FROM r
        LIMIT 3000000
      )
      SELECT i FROM r WHERE i = 1;
    SQL
  end

  def test_default_gvl_release_threshold
    db = Extralite::Database.new(':memory:')
    assert_equal 1000, db.gvl_release_threshold
  end

  def test_gvl_always_release
    skip if !IS_LINUX

    delays = []
    running = true
    t1 = Thread.new do
      last = Time.now
      while running
        sleep 0.1
        now = Time.now
        delays << (now - last)
        last = now
      end
    end
    t2 = Thread.new do
      db = Extralite::Database.new(':memory:')
      db.gvl_release_threshold = 1
      db.query(@sql)
    ensure
      running = false
    end
    t2.join
    t1.join

    assert delays.size > 4
    assert_equal 0, delays.select { |d| d > 0.15 }.size
  ensure
    t1&.kill
    t2&.kill
  end

  def test_gvl_always_hold
    skip if !IS_LINUX

    delays = []
    running = true

    signal = Queue.new
    db = Extralite::Database.new(':memory:')
    db.gvl_release_threshold = 0

    t1 = Thread.new do
      last = Time.now
      while running
        signal << true
        sleep 0.1
        now = Time.now
        delays << (now - last)
        last = now
      end
    end

    t2 = Thread.new do
      signal.pop
      db.query(@sql)
    ensure
      running = false
    end
    t2.join
    t1.join

    assert delays.size >= 1
    assert delays.first > 0.2
  ensure
    t1&.kill
    t2&.kill
  end

  def test_gvl_mode_get_set
    db = Extralite::Database.new(':memory:')
    assert_equal 1000, db.gvl_release_threshold

    db.gvl_release_threshold = 42
    assert_equal 42, db.gvl_release_threshold

    db.gvl_release_threshold = 0
    assert_equal 0, db.gvl_release_threshold

    assert_raises(ArgumentError) { db.gvl_release_threshold = :foo }

    db.gvl_release_threshold = nil
    assert_equal 1000, db.gvl_release_threshold
  end

  def test_progress_handler_simple
    db = Extralite::Database.new(':memory:')

    # SQLite's behaviour post 3.46.0 has changed, such that the first time the
    # query is ran, the progress handler is called more times than in later
    # invocations, so here we run it once before measuring, in order to have
    # reliable figures.
    result = db.query_single('select 1 as a, 2 as b, 3 as c')

    buf = []
    db.on_progress(period: 1) { buf << :progress }

    result = db.query_single('select 1 as a, 2 as b, 3 as c')
    assert_equal({ a: 1, b: 2, c: 3 }, result)
    assert_in_range 5..7, buf.size

    buf = []
    db.on_progress(period: 2) { buf << :progress }

    result = db.query_single('select 1 as a, 2 as b, 3 as c')
    assert_equal({ a: 1, b: 2, c: 3 }, result)
    assert_in_range 2..4, buf.size
  end

  def test_progress_handler_normal_mode
    db = Extralite::Database.new(':memory:')

    # SQLite's behaviour post 3.46.0 has changed, such that the first time the
    # query is ran, the progress handler is called more times than in later
    # invocations, so here we run it once before measuring, in order to have
    # reliable figures.
    db.query('select 1 as a')

    count = 0
    db.on_progress(period: 1) { count += 1 }
    db.query('select 1 as a')
    assert count > 0
    base_count = count

    count = 0
    db.on_progress(period: 1) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal base_count * 10, count

    count = 0
    db.on_progress(period: 10, tick: 1) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal base_count, count
  end

  def test_progress_handler_at_least_once_mode
    db = Extralite::Database.new(':memory:')

    # SQLite's behaviour post 3.46.0 has changed, such that the first time the
    # query is ran, the progress handler is called more times than in later
    # invocations, so here we run it once before measuring, in order to have
    # reliable figures.
    db.query('select 1 as a')

    count = 0
    db.on_progress(period: 1) { count += 1 }
    db.query('select 1 as a')
    assert count > 0
    base_count = count

    count = 0
    db.on_progress(period: 1, mode: :at_least_once) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal base_count * 10 + 10, count

    count = 0
    db.on_progress(period: 10, tick: 1, mode: :at_least_once) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal base_count + 10, count
  end

  def test_progress_handler_once_mode
    db = Extralite::Database.new(':memory:')

    count = 0
    db.on_progress(mode: :once) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal 10, count

    count = 0
    db.on_progress(period: 1, mode: :once) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal 10, count

    count = 0
    db.on_progress(period: 10, tick: 1, mode: :once) { count += 1 }
    10.times { db.query('select 1 as a') }
    assert_equal 10, count
  end

  def test_progress_handler_once_mode_with_batch_query
    db = Extralite::Database.new(':memory:')

    count = 0
    db.on_progress(period: 1, mode: :once) { count += 1 }
    db.batch_query('select ?', 1..10)
    assert_equal 10, count

    db.batch_query('select ?', 1..3)
    assert_equal 13, count
  end

  def test_progress_handler_reset
    db = Extralite::Database.new(':memory:')

    count = 0
    set_progress = -> {
      count = 0
      db.on_progress(mode: :once) { count += 1 }
    }

    set_progress.()
    10.times { db.query('select 1 as a') }
    assert_equal 10, count

    count = 0
    db.on_progress(mode: :none)
    10.times { db.query('select 1 as a') }
    assert_equal 0, count

    set_progress.()
    10.times { db.query('select 1 as a') }
    assert_equal 10, count

    count = 0
    db.on_progress(period: 0) { foo }
    10.times { db.query('select 1 as a') }
    assert_equal 0, count

    set_progress.()
    10.times { db.query('select 1 as a') }
    assert_equal 10, count

    count = 0
    db.on_progress
    10.times { db.query('select 1 as a') }
    assert_equal 0, count
  end

  def test_progress_handler_invalid_arg
    db = Extralite::Database.new(':memory:')

    assert_raises(TypeError) { db.on_progress(period: :foo) }
    assert_raises(TypeError) { db.on_progress(tick: :foo) }
    assert_raises(ArgumentError) { db.on_progress(mode: :foo) }
  end

  def test_progress_handler_once_mode_with_prepared_query
    db = Extralite::Database.new(':memory:')
    db.execute 'create table foo (x)'
    db.batch_query('insert into foo values (?)', 1..10)
    q = db.prepare('select x from foo')

    count = 0
    db.on_progress(period: 1, mode: :once) { count += 1 }

    q.to_a
    assert_equal 1, count

    q.reset
    record_count = 0
    assert_equal 2, count
    while q.next
      record_count += 1
    end
    assert_equal 10, record_count
    assert_equal 2, count

    q.reset
    assert_equal 3, count
    while q.next
      record_count += 1
    end
    assert_equal 3, count
  end

  LONG_QUERY = <<~SQL
    WITH RECURSIVE
      fibo (curr, next)
    AS
    ( SELECT 1,1
      UNION ALL
      SELECT next, curr + next FROM fibo
      LIMIT 10000000 )
    SELECT curr, next FROM fibo LIMIT 1 OFFSET 10000000-1;
  SQL

  def test_progress_handler_timeout_interrupt
    db = Extralite::Database.new(':memory:')
    t0 = Time.now
    db.on_progress do
      Thread.pass
      db.interrupt if Time.now - t0 >= 0.2
    end

    q = db.prepare(LONG_QUERY)
    result = nil
    err = nil
    begin
      result = q.next
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of Extralite::InterruptError, err

    # try a second time, just to make sure no undefined state is left behind
    t0 = Time.now
    q = db.prepare(LONG_QUERY)
    result = nil
    err = nil
    begin
      result = q.next
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of Extralite::InterruptError, err
  end

  class CustomTimeoutError < RuntimeError
  end

  def test_progress_handler_timeout_raise
    db = Extralite::Database.new(':memory:')
    t0 = Time.now
    db.on_progress do
      Thread.pass
      raise CustomTimeoutError if Time.now - t0 >= 0.2
    end

    q = db.prepare(LONG_QUERY)
    result = nil
    err = nil
    begin
      result = q.next
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of CustomTimeoutError, err

    # try a second time, just to make sure no undefined state is left behind
    t0 = Time.now
    q = db.prepare(LONG_QUERY)
    result = nil
    err = nil
    begin
      result = q.next
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of CustomTimeoutError, err
  end

  def test_progress_handler_busy_timeout
    fn = Tempfile.new('extralite_test_progress_handler_busy_timeout').path
    db1 = Extralite::Database.new(fn)
    db2 = Extralite::Database.new(fn)

    db1.query('begin exclusive')
    assert_raises(Extralite::BusyError) { db2.query('begin exclusive') }

    t0 = Time.now
    db2.on_progress do
      Thread.pass
      raise CustomTimeoutError if Time.now - t0 >= 0.2
    end

    result = nil
    err = nil
    begin
      result = db2.execute('begin exclusive')
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of CustomTimeoutError, err

    # Try a second time, to ensure no undefined state remains behind
    t0 = Time.now
    result = nil
    err = nil
    begin
      result = db2.execute('begin exclusive')
    rescue => e
      err = e
    end
    t1 = Time.now

    assert_nil result
    assert_equal 1, ((t1 - t0) * 5).round.to_i
    assert_kind_of CustomTimeoutError, err
  end

  def test_global_progress_handler
    count = 0
    Extralite.on_progress(tick: 1, period: 1) { count += 1 }

    db = Extralite::Database.new(':memory:')
    10.times { db.query('select 1') }
    refute_equal 0, count

    old_count = count
    Extralite.on_progress # remove global progress handler

    # already opened db should preserve progress handler behaviour
    10.times { db.query('select 1') }
    refute_equal old_count, count

    old_count = count
    db2 = Extralite::Database.new(':memory:')
    10.times { db2.query('select 1') }
    assert_equal old_count, count
  ensure
    Extralite.on_progress(mode: :none)
  end
end

class RactorTest < Minitest::Test
  def test_ractor_simple
    skip if SKIP_RACTOR_TESTS

    fn = Tempfile.new('extralite_test_database_in_ractor').path

    r = Ractor.new do
      path = receive
      db = Extralite::Database.new(path)
      i = receive
      db.execute 'insert into foo values (?)', i
    end

    r << fn
    db = Extralite::Database.new(fn)
    db.execute 'create table foo (x)'
    r << 42
    r.take # wait for ractor to terminate

    assert_equal 42, db.query_single_splat('select x from foo')
  end

  # Adapted from here: https://github.com/sparklemotion/sqlite3-ruby/pull/365/files
  def test_ractor_share_database
    skip "skipped for now as ractors seem kinda flakey (failing sporadically)"

    db_receiver = Ractor.new do
      db = Ractor.receive
      Ractor.yield db.object_id
      begin
        db.execute('create table foo (b)')
        raise 'Should have raised an exception in db.execute()'
      rescue => e
        Ractor.yield e
      end
    end
    sleep 0.1
    db_creator = Ractor.new(db_receiver) do |db_receiver|
      db = Extralite::Database.new(':memory:')
      Ractor.yield db.object_id
      db_receiver.send(db)
      sleep 0.1
      db.execute('create table foo (a)')
    end
    first_oid = db_creator.take
    second_oid = db_receiver.take
    refute_equal first_oid, second_oid
    ex = db_receiver.take
    assert_kind_of Extralite::Error, ex
    assert_equal 'Database is closed', ex.message
  end

  # Adapted from here: https://github.com/sparklemotion/sqlite3-ruby/pull/365/files
  def test_ractor_stress
    skip if SKIP_RACTOR_TESTS

    fn = Tempfile.new('extralite_test_ractor_stress').path
    random = Random.new.freeze

    ractors = (0..9).map do |ractor_number|
      sleep 0.05
      Ractor.new(fn, random, ractor_number) do |rfn, r, n|
        rdb = Extralite::Database.new(rfn)
        rdb.busy_timeout = 3
        rdb.pragma(journal_mode: 'wal', synchronous: 1)
        rdb.execute('create table if not exists stress_test (a integer primary_key, b text)')
        changes = 0
        10.times do |i|
          changes += rdb.execute('insert into stress_test(a, b) values (?, ?)', n * 100 + i, r.rand)
        end
        Ractor.yield changes
      end
    end

    buf = []
    ractors.each { |r| buf << r.take }
    assert_equal [10] * 10, buf

    final_check = Ractor.new(fn) do |rfn|
      rdb = Extralite::Database.new(rfn, wal: true)
      count = rdb.query_single_splat('select count(*) from stress_test')
      Ractor.yield count
    end
    count = final_check.take
    assert_equal 100, count
  end
end

class DatabaseTransformTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.query('create table t (a, b, c)')

    @q3 = @db.prepare('select * from t where a = ?')
    @q4 = @db.prepare('select * from t order by a')

    @db.batch_execute('insert into t (a, b, c) values (?, ?, ?)', [
      [1, 2, { foo: 42, bar: 43 }.to_json],
      [4, 5, { foo: 45, bar: 46 }.to_json]
    ])
  end

  class MyModel
    def initialize(h)
      @h = h
    end

    def values
      @h
    end
  end

  def test_query_hash_transform
    transform = ->(h) { MyModel.new(h) }

    sql = 'select a, b from t where a = ?'
    o = @db.query(transform, sql, 1).first
    assert_kind_of MyModel, o
    assert_equal({ a: 1, b: 2 }, o.values)

    o = @db.query(transform, sql, 4).first
    assert_kind_of MyModel, o
    assert_equal({ a: 4, b: 5 }, o.values)

    sql = 'select a, b from t order by a'
    assert_equal [
      { a: 1, b: 2 },
      { a: 4, b: 5 }
    ], @db.query(transform, sql).map(&:values)

    buf = []
    @db.query(transform, sql) { |r| buf << r.values }
    assert_equal [
      { a: 1, b: 2 },
      { a: 4, b: 5 }
    ], buf
  end

  def test_query_array_transform
    transform = ->(h) { MyModel.new(h) }

    sql = 'select a, b from t where a = ?'
    o = @db.query_array(transform, sql, 1).first
    assert_kind_of MyModel, o
    assert_equal([1, 2], o.values)

    o = @db.query_array(transform, sql, 4).first
    assert_kind_of MyModel, o
    assert_equal([4, 5], o.values)

    sql = 'select a, b from t order by a'
    assert_equal [
      [1, 2],
      [4, 5]
    ], @db.query_array(transform, sql).map(&:values)

    buf = []
    @db.query_array(transform, sql) { |r| buf << r.values }
    assert_equal [
      [1, 2],
      [4, 5]
    ], buf
  end

  def test_query_hash_single_row_transform
    transform = ->(h) { MyModel.new(h) }

    sql = 'select a, b from t where a = ?'
    o = @db.query_single(transform, sql, 1)
    assert_kind_of MyModel, o
    assert_equal({ a: 1, b: 2 }, o.values)

    o = @db.query_single(transform, sql, 4)
    assert_kind_of MyModel, o
    assert_equal({ a: 4, b: 5 }, o.values)
  end

  def test_query_array_single_row_transform
    transform = ->(h) { MyModel.new(h) }

    sql = 'select a, b from t where a = ?'
    o = @db.query_single_array(transform, sql, 1)
    assert_kind_of MyModel, o
    assert_equal([1, 2], o.values)

    o = @db.query_single_array(transform, sql, 4)
    assert_kind_of MyModel, o
    assert_equal([4, 5], o.values)
  end

  def test_query_splat_single_column_transform
    transform = ->(c) { JSON.parse(c, symbolize_names: true) }
    sql = 'select c from t where a = ?'

    assert_equal([{ foo: 42, bar: 43 }], @db.query_splat(transform, sql, 1))
    assert_equal([{ foo: 45, bar: 46 }], @db.query_splat(transform, sql, 4))

    sql = 'select c from t order by a'
    assert_equal [
      { foo: 42, bar: 43 },
      { foo: 45, bar: 46 }
    ], @db.query_splat(transform, sql)

    buf = []
    @db.query_splat(transform, sql) { |r| buf << r }
    assert_equal [
      { foo: 42, bar: 43 },
      { foo: 45, bar: 46 }
    ], buf
  end

  def test_query_splat_single_row_single_column
    transform = ->(c) { JSON.parse(c, symbolize_names: true) }
    sql = 'select c from t where a = ?'

    assert_equal({ foo: 42, bar: 43 }, @db.query_single_splat(transform, sql, 1))
    assert_equal({ foo: 45, bar: 46 }, @db.query_single_splat(transform, sql, 4))
  end

  def test_query_splat_multi_column
    transform = ->(a, b, c) { { a: a, b: b, c: JSON.parse(c, symbolize_names: true) } }
    sql = 'select * from t where a = ?'

    assert_equal([{ a: 1, b: 2, c: { foo: 42, bar: 43 }}], @db.query_splat(transform, sql, 1))
    assert_equal([{ a: 4, b: 5, c: { foo: 45, bar: 46 }}], @db.query_splat(transform, sql, 4))

    sql = 'select * from t order by a'
    assert_equal [
      { a: 1, b: 2, c: { foo: 42, bar: 43 }},
      { a: 4, b: 5, c: { foo: 45, bar: 46 }}
    ], @db.query_splat(transform, sql)

    buf = []
    @db.query_splat(transform, sql) { |r| buf << r }
    assert_equal [
      { a: 1, b: 2, c: { foo: 42, bar: 43 }},
      { a: 4, b: 5, c: { foo: 45, bar: 46 }}
    ], buf
  end

  def test_query_splat_single_row_multi_column
    transform = ->(a, b, c) { { a: a, b: b, c: JSON.parse(c, symbolize_names: true) } }
    sql = 'select * from t where a = ?'

    assert_equal({ a: 1, b: 2, c: { foo: 42, bar: 43 }}, @db.query_single_splat(transform, sql, 1))
    assert_equal({ a: 4, b: 5, c: { foo: 45, bar: 46 }}, @db.query_single_splat(transform, sql, 4))
  end
end
