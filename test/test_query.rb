# frozen_string_literal: true

require_relative 'helper'
require 'date'
require 'json'

class QueryTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.query('create table if not exists t (x,y,z)')
    @db.query('delete from t')
    @db.query('insert into t values (1, 2, 3)')
    @db.query('insert into t values (4, 5, 6)')
    @db.query('insert into t values (7, 8, 9)')

    @query = @db.prepare('select * from t where x = ?')
  end

  def test_prepare
    query = @db.prepare('select 1')
    assert_equal({ '1': 1 }, query.next)

    query = @db.prepare('select x from t where y = ?', 5)
    assert_equal({ x: 4 }, query.next)
  end

  def test_mode
    query = @db.prepare('select 1')
    assert_equal :hash, query.mode
    
    query.mode = :argv
    assert_equal :argv, query.mode

    query.mode = :ary
    assert_equal :ary, query.mode

    assert_raises(Extralite::Error) { query.mode = :foo }
    assert_equal :ary, query.mode

    query.mode = :hash
    assert_equal :hash, query.mode
  end

  def test_prepare_argv
    query = @db.prepare_argv('select 1')
    assert_equal :argv, query.mode

    assert_equal 1, query.next
  end

  def test_prepare_argv_with_too_many_columns
    q = @db.prepare_argv('select 1, 2, 3, 4, 5, 6, 7, 8, 9')

    assert_raises(Extralite::Error) { q.next }
  end

  def test_prepare_ary
    query = @db.prepare_ary('select 1')
    assert_equal :ary, query.mode

    assert_equal [1], query.next
  end

  def test_query_props
    assert_kind_of Extralite::Query, @query
    assert_equal @db, @query.database
    assert_equal @db, @query.db
    assert_equal 'select * from t where x = ?', @query.sql
  end

  def test_bind
    @db.query("insert into t values ('a', 'b', 'c')")

    q = @db.prepare_ary('select * from t where `z` = :foo')
    results = q.bind(foo: 'c').to_a

    assert_equal [['a', 'b', 'c']], results

    # try again with the same parameters
    results = q.to_a

    assert_equal [['a', 'b', 'c']], results
  end

  def test_query_next
    query = @db.prepare('select * from t')
    v = query.next
    assert_equal({ x: 1, y: 2, z: 3}, v)

    v = query.next
    assert_equal({ x: 4, y: 5, z: 6}, v)

    v = query.next
    assert_equal({ x: 7, y: 8, z: 9}, v)

    v = query.next
    assert_nil v

    # EOF should repeat
    v = query.next
    assert_nil v
  end

  def test_query_next_with_row_count
    query = @db.prepare('select * from t')
    v = query.next(1)
    assert_equal([{ x: 1, y: 2, z: 3}], v)

    v = query.next(2)
    assert_equal([{ x: 4, y: 5, z: 6}, { x: 7, y: 8, z: 9}], v)

    v = query.next
    assert_nil v

    query = @db.prepare('select * from t')
    v = query.next(-1)
    assert_equal([{ x: 1, y: 2, z: 3}, { x: 4, y: 5, z: 6}, { x: 7, y: 8, z: 9}], v)
  end

  def test_query_next_with_block
    query = @db.prepare('select * from t')
    buf = []
    v = query.next { |r| buf << r }
    assert_equal query, v
    assert_equal [{x: 1, y: 2, z: 3}], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [{ x: 4, y: 5, z: 6}, { x: 7, y: 8, z: 9}], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_next_with_block_with_break
    query = @db.prepare('select * from t')
    buf = []
    v = query.next(-1) { |r| buf << r; break }
    assert_nil v
    assert_equal [{x: 1, y: 2, z: 3}], buf

    buf = []
    v = query.next(-1) { |r| buf << r }
    assert_equal query, v
    assert_equal [{x: 4, y: 5, z: 6}, {x: 7, y: 8, z: 9}], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_next_ary
    query = @db.prepare_ary('select * from t')
    v = query.next
    assert_equal([1, 2, 3], v)

    v = query.next
    assert_equal([4, 5, 6], v)

    v = query.next
    assert_equal([7, 8, 9], v)

    v = query.next
    assert_nil v

    v = query.next
    assert_nil v
  end

  def test_query_next_ary_with_row_count
    query = @db.prepare_ary('select * from t')
    v = query.next(1)
    assert_equal([[1, 2, 3]], v)

    v = query.next(2)
    assert_equal([[4, 5, 6], [7, 8, 9]], v)

    v = query.next(2)
    assert_equal([], v)

    v = query.next(2)
    assert_nil v
  end

  def test_query_next_ary_with_block
    query = @db.prepare_ary('select * from t')
    buf = []
    v = query.next { |r| buf << r }
    assert_equal query, v
    assert_equal [[1, 2, 3]], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [[4, 5, 6], [7, 8, 9]], buf

    buf = []
    v = query.next(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_next_argv_single_column
    query = @db.prepare_argv('select x from t')
    v = query.next
    assert_equal(1, v)

    v = query.next
    assert_equal(4, v)

    v = query.next
    assert_equal(7, v)

    v = query.next
    assert_nil v
  end

  def test_query_next_argv_multi_column
    query = @db.prepare_argv('select x, y from t')
    v = query.next
    assert_equal([1, 2], v)

    v = query.next
    assert_equal([4, 5], v)

    v = query.next
    assert_equal([7, 8], v)

    v = query.next
    assert_nil v
  end

  def test_query_next_argv_with_row_count
    query = @db.prepare_argv('select x from t')
    v = query.next(1)
    assert_equal([1], v)

    v = query.next(3)
    assert_equal([4, 7], v)

    v = query.next(2)
    assert_nil v
  end

  def test_query_next_argv_with_block
    query = @db.prepare_argv('select x, y from t')
    buf = []
    v = query.next { |x, y| buf << [x, y] }
    assert_equal query, v
    assert_equal [[1, 2]], buf

    buf = []
    v = query.next(2) { |x, y| buf << [x, y] }
    assert_equal query, v
    assert_equal [[4, 5], [7, 8]], buf

    buf = []
    v = query.next(2) { |x, y| buf << [x, y] }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_to_a
    assert_equal [{ x: 1, y: 2, z: 3 }], @query.bind(1).to_a
    assert_equal [{ x: 4, y: 5, z: 6 }], @query.bind(4).to_a

    @query.mode = :ary

    assert_equal [[1, 2, 3]], @query.bind(1).to_a
    assert_equal [[4, 5, 6]], @query.bind(4).to_a

    query = @db.prepare_argv('select y from t')
    assert_equal [2, 5, 8], query.to_a
  end

  def test_query_each
    buf = []
    @query.bind(1).each { |r| buf << r }
    assert_equal [{x: 1, y: 2, z: 3}], buf

    # each should reset the stmt
    buf = []
    @query.each { |r| buf << r }
    assert_equal [{x: 1, y: 2, z: 3}], buf

    query = @db.prepare('select * from t')
    buf = []
    query.each { |r| buf << r }
    assert_equal [{x: 1, y: 2, z: 3},{ x: 4, y: 5, z: 6 }, { x: 7, y: 8, z: 9 }], buf
  end

  def test_query_each_with_break
    query = @db.prepare('select * from t')

    buf = []
    query.each { |r| buf << r; break }
    assert_equal [{x: 1, y: 2, z: 3}], buf

    # each should reset the stmt
    buf = []
    query.each { |r| buf << r }
    assert_equal [{x: 1, y: 2, z: 3},{ x: 4, y: 5, z: 6 }, { x: 7, y: 8, z: 9 }], buf
  end

  def test_query_each_without_block
    query = @db.prepare('select * from t')
    iter = query.each
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [{x: 1, y: 2, z: 3},{ x: 4, y: 5, z: 6 }, { x: 7, y: 8, z: 9 }], buf
  end

  def test_query_each_ary
    buf = []
    @query.mode = :ary
    @query.bind(1).each { |r| buf << r }
    assert_equal [[1, 2, 3]], buf

    # each should reset the stmt
    buf = []
    @query.each { |r| buf << r }
    assert_equal [[1, 2, 3]], buf

    query = @db.prepare_ary('select * from t')
    buf = []
    query.each { |r| buf << r }
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_ary_without_block
    query = @db.prepare_ary('select * from t')
    iter = query.each
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_argv
    buf = []
    @query.mode = :argv
    @query.bind(1).each { |a, b, c| buf << [a, b, c] }
    assert_equal [[1, 2, 3]], buf

    # each should reset the stmt
    buf = []
    @query.each { |a, b, c| buf << [a, b, c] }
    assert_equal [[1, 2, 3]], buf

    query = @db.prepare_argv('select * from t')
    buf = []
    query.each { |a, b, c| buf << [a, b, c] }
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_argv_without_block
    query = @db.prepare_argv('select * from t')
    iter = query.each
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |a, b, c| buf << [a, b, c] }
    assert_equal iter, v
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_argv_single_column
    query = @db.prepare_argv('select x from t where x = ?')
    buf = []
    query.bind(1).each { |r| buf << r }
    assert_equal [1], buf

    # each should reset the stmt
    buf = []
    query.each { |r| buf << r }
    assert_equal [1], buf

    query = @db.prepare_argv('select x from t')
    buf = []
    query.each { |r| buf << r }
    assert_equal [1, 4, 7], buf
  end

  def test_query_each_argv_single_column_without_block
    query = @db.prepare_argv('select x from t')
    iter = query.each
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [1, 4, 7], buf
  end

  def test_query_with_invalid_sql
    assert_raises(Extralite::SQLError) { @db.prepare('blah').to_a }
  end

  def test_query_with_multiple_queries
    assert_raises(Extralite::Error) { @db.prepare('select 1; select 2').to_a }
  end

  def test_query_multiple_statements
    assert_raises(Extralite::Error) {
      @db.prepare("insert into t values ('a', 'b', 'c'); insert into t values ('d', 'e', 'f');").to_a
    }
  end

  def test_query_multiple_statements_with_bad_sql
    error = nil
    begin
      query =@db.prepare("insert into t values foo; insert into t values ('d', 'e', 'f');")
      query.next
    rescue => error
    end

    assert_kind_of Extralite::SQLError, error
    assert_equal 'near "foo": syntax error', error.message
  end

  def test_query_repeated_execution_missing_param
    assert_nil @query.next

    @query.bind(4)
    assert_equal({x: 4, y: 5, z: 6}, @query.next)
  end

  def test_query_empty_sql
    assert_raises(Extralite::Error) { @db.prepare(' ') }

    r = @db.prepare('select 1 as foo;  ').next
    assert_equal({ foo: 1 }, r)
  end

  def test_query_parameter_binding_simple
    r = @db.prepare('select x, y, z from t where x = ?').bind(1).next
    assert_equal({ x: 1, y: 2, z: 3 }, r)

    error = assert_raises(Extralite::ParameterError) { @db.prepare('select ?').bind(Date.today).next }
    assert_equal error.message, 'Cannot bind parameter at position 1 of type Date'
  end

  def test_query_parameter_binding_with_index
    r = @db.prepare('select x, y, z from t where x = ?2').bind(0, 1).next
    assert_equal({ x: 1, y: 2, z: 3 }, r)

    r = @db.prepare('select x, y, z from t where z = ?3').bind(3, 4, 6).next
    assert_equal({ x: 4, y: 5, z: 6 }, r)
  end

  def test_query_parameter_binding_with_name
    r = @db.prepare('select x, y, z from t where x = :x').bind(x: 1, y: 2).next
    assert_equal({ x: 1, y: 2, z: 3 }, r)

    r = @db.prepare('select x, y, z from t where z = :zzz').bind('zzz' => 6).next
    assert_equal({ x: 4, y: 5, z: 6 }, r)

    r = @db.prepare('select x, y, z from t where z = :bazzz').bind(':bazzz' => 6).next
    assert_equal({ x: 4, y: 5, z: 6 }, r)
  end

  def test_query_parameter_binding_with_index_key
    r = @db.prepare('select x, y, z from t where z = ?').bind(1 => 3).next
    assert_equal({ x: 1, y: 2, z: 3 }, r)

    r = @db.prepare('select x, y, z from t where x = ?2').bind(1 => 42, 2 => 4).next
    assert_equal({ x: 4, y: 5, z: 6 }, r)
  end

  class Foo; end

  def test_parameter_binding_from_hash
    assert_equal 42, @db.prepare_argv('select :bar').bind(foo: 41, bar: 42).next
    assert_equal 42, @db.prepare_argv('select :bar').bind('foo' => 41, 'bar' => 42).next
    assert_equal 42, @db.prepare_argv('select ?8').bind(7 => 41, 8 => 42).next
    assert_nil @db.prepare_argv('select :bar').bind(foo: 41).next

    error = assert_raises(Extralite::ParameterError) { @db.prepare_argv('select ?').bind(Foo.new => 42).next }
    assert_equal error.message, 'Cannot bind parameter with a key of type QueryTest::Foo'

    error = assert_raises(Extralite::ParameterError) { @db.prepare_argv('select ?').bind(%w[a b] => 42).next }
    assert_equal error.message, 'Cannot bind parameter with a key of type Array'
  end

  def test_parameter_binding_from_struct
    foo_bar = Struct.new(:':foo', :bar)
    value = foo_bar.new(41, 42)
    assert_equal 41, @db.prepare_argv('select :foo').bind(value).next
    assert_equal 42, @db.prepare_argv('select :bar').bind(value).next
    assert_nil @db.prepare_argv('select :baz').bind(value).next
  end

  def test_parameter_binding_from_data_class
    skip "Data isn't supported in Ruby < 3.2" if RUBY_VERSION < '3.2'

    foo_bar = Data.define(:':foo', :bar)
    value = foo_bar.new(':foo': 41, bar: 42)
    assert_equal 42, @db.prepare_argv('select :bar').bind(value).next
    assert_nil @db.prepare_argv('select :baz').bind(value).next
  end

  def test_query_columns
    r = @db.prepare("select 'abc' as a, 'def' as b").columns
    assert_equal [:a, :b], r
  end

  def test_query_columns_with_parameterized_sql
    q = @db.prepare_ary('select * from t where z = :z')
    q.bind(z: 9)
    assert_equal [:x, :y, :z], q.columns
    assert_equal [[7, 8, 9]], q.to_a
  end

  def test_query_close
    p = @db.prepare("select 'abc'")

    assert_equal false, p.closed?

    p.close
    assert_equal true, p.closed?

    p.close
    assert_equal true, p.closed?

    assert_raises(Extralite::Error) { p.next }
  end

  def test_query_execute
    q = @db.prepare('update t set x = 42')
    assert_equal 3, q.execute
    assert_equal [[42, 2, 3], [42, 5, 6], [42, 8, 9]], @db.query_ary('select * from t order by z')
  end

  def test_query_execute_with_params
    q = @db.prepare('update t set x = ? where z = ?')
    assert_equal 1, q.execute(42, 9)
    assert_equal [[1, 2, 3], [4, 5, 6], [42, 8, 9]], @db.query_ary('select * from t order by z')
  end

  def test_query_execute_with_mixed_params
    @db.execute 'delete from t'
    q = @db.prepare('insert into t values (?, ?, ?)')
    
    q.execute(1, [2], 3)
    q.execute([4, 5], 6)
    q.execute([7], 8, [9])

    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], @db.query_ary('select * from t order by z')
  end

  def test_query_chverons
    q = @db.prepare('update t set x = ? where z = ?')
    assert_equal q, (q << [42, 9])
    assert_equal [[1, 2, 3], [4, 5, 6], [42, 8, 9]], @db.query_ary('select * from t order by z')
  end


  def test_query_batch_execute
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    records = [
      [1, '2', 3],
      ['4', 5, 6]
    ]

    p = @db.prepare('insert into foo values (?, ?, ?)')
    changes = p.batch_execute(records)

    assert_equal 2, changes
    assert_equal [
      { a: 1, b: '2', c: 3 },
      { a: '4', b: 5, c: 6 }
    ], @db.query('select * from foo')
  end

  def test_query_batch_execute_with_each_interface
    @db.query('create table foo (a)')
    assert_equal [], @db.query('select * from foo')

    p = @db.prepare('insert into foo values (?)')
    changes = p.batch_execute(1..3)

    assert_equal 3, changes
    assert_equal [
      { a: 1 },
      { a: 2 },
      { a: 3 }
    ], @db.query('select * from foo')
  end

  def test_query_batch_execute_with_proc
    source = [42, 43, 44]

    @db.query('create table foo (a)')
    assert_equal [], @db.query('select * from foo')

    p = @db.prepare('insert into foo values (?)')
    pr = proc { source.shift }
    changes = p.batch_execute(pr)

    assert_equal 3, changes
    assert_equal [
      { a: 42 },
      { a: 43 },
      { a: 44 }
    ], @db.query('select * from foo')
  end

  def test_query_batch_query_with_array
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare('update foo set b = ? returning *')

    results = q.batch_query([42, 43])
    assert_equal [
      [{ a: 1, b: 42 }, { a: 2, b: 42 }, { a: 3, b: 42 }],
      [{ a: 1, b: 43 }, { a: 2, b: 43 }, { a: 3, b: 43 }]
    ], results

    array = []
    changes = q.batch_query([44, 45]) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [{ a: 1, b: 44 }, { a: 2, b: 44 }, { a: 3, b: 44 }],
      [{ a: 1, b: 45 }, { a: 2, b: 45 }, { a: 3, b: 45 }]
    ], array
  end

  def test_query_batch_query_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare('update foo set b = ? returning *')

    results = q.batch_query(42..43)
    assert_equal [
      [{ a: 1, b: 42 }, { a: 2, b: 42 }, { a: 3, b: 42 }],
      [{ a: 1, b: 43 }, { a: 2, b: 43 }, { a: 3, b: 43 }]
    ], results

    array = []
    changes = q.batch_query(44..45) do |rows|
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

  def test_query_batch_query_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare('update foo set b = ? returning *')

    pr = parameter_source_proc([42, 43])
    results = q.batch_query(pr)
    assert_equal [
      [{ a: 1, b: 42 }, { a: 2, b: 42 }, { a: 3, b: 42 }],
      [{ a: 1, b: 43 }, { a: 2, b: 43 }, { a: 3, b: 43 }]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = q.batch_query(pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [{ a: 1, b: 44 }, { a: 2, b: 44 }, { a: 3, b: 44 }],
      [{ a: 1, b: 45 }, { a: 2, b: 45 }, { a: 3, b: 45 }]
    ], array
  end

  def test_query_batch_query_ary_with_array
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_ary('update foo set b = ? returning *')

    results = q.batch_query([42, 43])
    assert_equal [
      [[1, 42], [2, 42], [3, 42]],
      [[1, 43], [2, 43], [3, 43]]
    ], results

    array = []
    changes = q.batch_query([44, 45]) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [[1, 44], [2, 44], [3, 44]],
      [[1, 45], [2, 45], [3, 45]]
    ], array
  end

  def test_query_batch_query_ary_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_ary('update foo set b = ? returning *')

    results = q.batch_query(42..43)
    assert_equal [
      [[1, 42], [2, 42], [3, 42]],
      [[1, 43], [2, 43], [3, 43]]
    ], results

    array = []
    changes = q.batch_query(44..45) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [[1, 44], [2, 44], [3, 44]],
      [[1, 45], [2, 45], [3, 45]]
    ], array
  end

  def test_query_batch_query_ary_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_ary('update foo set b = ? returning *')

    pr = parameter_source_proc([42, 43])
    results = q.batch_query(pr)
    assert_equal [
      [[1, 42], [2, 42], [3, 42]],
      [[1, 43], [2, 43], [3, 43]]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = q.batch_query(pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [[1, 44], [2, 44], [3, 44]],
      [[1, 45], [2, 45], [3, 45]]
    ], array
  end

  def test_query_batch_query_single_column_with_array
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_argv('update foo set b = ? returning b * 10 + a')

    results = q.batch_query([42, 43])
    assert_equal [
      [421, 422, 423],
      [431, 432, 433]
    ], results

    array = []
    changes = q.batch_query([44, 45]) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [441, 442, 443],
      [451, 452, 453]
    ], array
  end

  def test_query_batch_query_single_column_with_enumerable
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_argv('update foo set b = ? returning b * 10 + a')

    results = q.batch_query(42..43)
    assert_equal [
      [421, 422, 423],
      [431, 432, 433]
    ], results

    array = []
    changes = q.batch_query(44..45) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [441, 442, 443],
      [451, 452, 453]
    ], array
  end

  def test_query_batch_query_single_column_with_proc
    @db.query('create table foo (a integer primary key, b)')
    assert_equal [], @db.query('select * from foo')

    data = [5, 4, 3]
    results = @db.batch_query_ary('insert into foo (b) values (?) returning *', data)
    assert_equal [
      [[1, 5]],
      [[2, 4]],
      [[3, 3]]
    ], results

    q = @db.prepare_argv('update foo set b = ? returning b * 10 + a')

    pr = parameter_source_proc([42, 43])
    results = q.batch_query(pr)
    assert_equal [
      [421, 422, 423],
      [431, 432, 433]
    ], results

    array = []
    pr = parameter_source_proc([44, 45])
    changes = q.batch_query(pr) do |rows|
      array << rows
    end
    assert_equal 6, changes
    assert_equal [
      [441, 442, 443],
      [451, 452, 453]
    ], array
  end

  def test_query_status
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.to_a
    assert_equal 1, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.to_a
    assert_equal 2, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.to_a
    assert_equal 3, @query.status(Extralite::SQLITE_STMTSTATUS_RUN, true)
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
  end

  def test_query_status_after_close
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.to_a
    assert_equal 1, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.close
    assert_raises(Extralite::Error) { @query.status(Extralite::SQLITE_STMTSTATUS_RUN) }
  end

  def test_query_after_db_close
    assert_equal [{ x: 4, y: 5, z: 6}], @query.bind(4).to_a
    @db.close
    assert_equal [{ x: 4, y: 5, z: 6}], @query.bind(4).to_a
  end

  def test_query_eof
    query = @db.prepare_argv('select x from t')
    assert_equal false, query.eof?

    query.next
    assert_equal false, query.eof?

    query.next(2)
    assert_equal false, query.eof?

    query.next
    assert_equal true, query.eof?

    query.next
    assert_equal true, query.eof?

    query.reset
    assert_equal false, query.eof?

    query.next
    assert_equal false, query.eof?

    assert_equal [1, 4, 7], query.to_a
    assert_equal true, query.eof?
  end

  def test_query_inspect
    q = @db.prepare('select x from t')
    assert_match /^\#\<Extralite::Query:0x[0-9a-f]+ #{q.sql.inspect}\>$/, q.inspect
  end

  def test_query_clone
    q1 = @db.prepare('select x from t')
    q2 = q1.clone

    assert_kind_of Extralite::Query, q2
    assert_equal @db, q2.database
    assert_equal q1.sql, q2.sql
    refute_equal q1, q2
  end

  def test_query_dup
    q1 = @db.prepare('select x from t')
    q2 = q1.dup

    assert_kind_of Extralite::Query, q2
    assert_equal @db, q2.database
    assert_equal q1.sql, q2.sql
    refute_same  q1, q2

    q1 = @db.prepare_argv('select x from t')
    q2 = q1.dup

    assert_kind_of Extralite::Query, q2
    assert_equal @db, q2.database
    assert_equal q1.sql, q2.sql
    refute_same  q1, q2
    assert_equal :argv, q2.mode
  end

  def test_query_dup_with_transform
    q1 = @db.prepare_ary('select x, y from t') { |a| a * 2 }
    q2 = q1.dup

    assert_equal [
      [1, 2, 1, 2],
      [4, 5, 4, 5],
      [7, 8, 7, 8],
    ], q2.to_a
  end
end

class QueryTransformTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.query('create table t (a, b, c)')

    @q1 = @db.prepare_argv('select c from t where a = ?')
    @q2 = @db.prepare_argv('select c from t order by a')
    
    @q3 = @db.prepare('select * from t where a = ?')
    @q4 = @db.prepare('select * from t order by a')

    @q5 = @db.prepare('select a, b from t where a = ?')
    @q6 = @db.prepare('select a, b from t order by a')

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

  def test_transform_hash
    q = @q5.transform { |h| MyModel.new(h) }
    assert_equal @q5, q

    o = @q5.bind(1).next
    assert_kind_of MyModel, o
    assert_equal({ a: 1, b: 2 }, o.values)

    o = @q5.bind(4).next
    assert_kind_of MyModel, o
    assert_equal({ a: 4, b: 5 }, o.values)

    assert_equal [
      [{ a: 1, b: 2 }],
      [{ a: 4, b: 5 }]
    ], @q5.batch_query([[1], [4]]).map { |a| a.map(&:values) }

    @q6.transform { |h| MyModel.new(h) }
    assert_equal [
      { a: 1, b: 2 },
      { a: 4, b: 5 }
    ], @q6.to_a.map(&:values)

    buf = []
    @q6.each { |r| buf << r.values }
    assert_equal [
      { a: 1, b: 2 },
      { a: 4, b: 5 }
    ], buf
  end

  def test_transform_ary
    @q5.mode = :ary
    q = @q5.transform { |h| MyModel.new(h) }
    assert_equal @q5, q

    o = @q5.bind(1).next
    assert_kind_of MyModel, o
    assert_equal([1, 2], o.values)

    o = @q5.bind(4).next
    assert_kind_of MyModel, o
    assert_equal([4, 5], o.values)

    assert_equal [
      [[1, 2]],
      [[4, 5]]
    ], @q5.batch_query([[1], [4]]).map { |a| a.map(&:values) }

    @q6.mode = :ary
    @q6.transform { |h| MyModel.new(h) }
    assert_equal [
      [1, 2],
      [4, 5]
    ], @q6.to_a.map(&:values)

    buf = []
    @q6.each { |r| buf << r.values }
    assert_equal [
      [1, 2],
      [4, 5]
    ], buf
  end

  def test_transform_argv_single_column
    q = @q1.transform { |c| JSON.parse(c, symbolize_names: true) }
    assert_equal @q1, q

    assert_equal({ foo: 42, bar: 43 }, @q1.bind(1).next)
    assert_equal({ foo: 45, bar: 46 }, @q1.bind(4).next)

    assert_equal [
      [{ foo: 42, bar: 43 }],
      [{ foo: 45, bar: 46 }]
    ], @q1.batch_query([[1], [4]])

    @q2.transform { |c| JSON.parse(c, symbolize_names: true) }
    assert_equal [
      { foo: 42, bar: 43 },
      { foo: 45, bar: 46 }
    ], @q2.to_a

    buf = []
    @q2.each { |r| buf << r }
    assert_equal [
      { foo: 42, bar: 43 },
      { foo: 45, bar: 46 }
    ], buf
  end

  def test_transform_argv_multi_column
    @q3.mode = :argv
    q = @q3.transform { |a, b, c| { a: a, b: b, c: JSON.parse(c, symbolize_names: true) } }
    assert_equal @q3, q

    assert_equal({ a: 1, b: 2, c: { foo: 42, bar: 43 }}, @q3.bind(1).next)
    assert_equal({ a: 4, b: 5, c: { foo: 45, bar: 46 }}, @q3.bind(4).next)

    assert_equal [
      [{ a: 1, b: 2, c: { foo: 42, bar: 43 }}],
      [{ a: 4, b: 5, c: { foo: 45, bar: 46 }}]
    ], @q3.batch_query([[1], [4]])

    @q4.mode = :argv
    @q4.transform { |a, b, c| { a: a, b: b, c: JSON.parse(c, symbolize_names: true) } }
    assert_equal [
      { a: 1, b: 2, c: { foo: 42, bar: 43 }},
      { a: 4, b: 5, c: { foo: 45, bar: 46 }}
    ], @q4.to_a

    buf = []
    @q4.each { |r| buf << r }
    assert_equal [
      { a: 1, b: 2, c: { foo: 42, bar: 43 }},
      { a: 4, b: 5, c: { foo: 45, bar: 46 }}
    ], buf
  end
end
