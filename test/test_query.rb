# frozen_string_literal: true

require_relative 'helper'

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
    assert_equal 1, query.next_single_column

    query = @db.prepare('select x from t where y = ?', 5)
    assert_equal 4, query.next_single_column
  end

  def test_query_props
    assert_kind_of Extralite::Query, @query
    assert_equal @db, @query.database
    assert_equal @db, @query.db
    assert_equal 'select * from t where x = ?', @query.sql
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

    query = @db.prepare('select * from t')
    v = query.next_hash
    assert_equal({ x: 1, y: 2, z: 3}, v)

    v = query.next_hash
    assert_equal({ x: 4, y: 5, z: 6}, v)

    v = query.next_hash
    assert_equal({ x: 7, y: 8, z: 9}, v)

    v = query.next_hash
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
    query = @db.prepare('select * from t')
    v = query.next_ary
    assert_equal([1, 2, 3], v)

    v = query.next_ary
    assert_equal([4, 5, 6], v)

    v = query.next_ary
    assert_equal([7, 8, 9], v)

    v = query.next_ary
    assert_nil v

    v = query.next_ary
    assert_nil v
  end

  def test_query_next_ary_with_row_count
    query = @db.prepare('select * from t')
    v = query.next_ary(1)
    assert_equal([[1, 2, 3]], v)

    v = query.next_ary(2)
    assert_equal([[4, 5, 6], [7, 8, 9]], v)

    v = query.next_ary(2)
    assert_equal([], v)

    v = query.next_ary(2)
    assert_nil v
  end

  def test_query_next_ary_with_block
    query = @db.prepare('select * from t')
    buf = []
    v = query.next_ary { |r| buf << r }
    assert_equal query, v
    assert_equal [[1, 2, 3]], buf

    buf = []
    v = query.next_ary(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [[4, 5, 6], [7, 8, 9]], buf

    buf = []
    v = query.next_ary(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_next_single_column
    query = @db.prepare('select x from t')
    v = query.next_single_column
    assert_equal(1, v)

    v = query.next_single_column
    assert_equal(4, v)

    v = query.next_single_column
    assert_equal(7, v)

    v = query.next_single_column
    assert_nil v
  end

  def test_query_next_single_column_with_row_count
    query = @db.prepare('select x from t')
    v = query.next_single_column(1)
    assert_equal([1], v)

    v = query.next_single_column(3)
    assert_equal([4, 7], v)

    v = query.next_single_column(2)
    assert_nil v
  end

  def test_query_next_single_column_with_block
    query = @db.prepare('select x from t')
    buf = []
    v = query.next_single_column { |r| buf << r }
    assert_equal query, v
    assert_equal [1], buf

    buf = []
    v = query.next_single_column(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [4, 7], buf

    buf = []
    v = query.next_single_column(2) { |r| buf << r }
    assert_equal query, v
    assert_equal [], buf
  end

  def test_query_to_a
    assert_equal [{ x: 1, y: 2, z: 3 }], @query.bind(1).to_a
    assert_equal [{ x: 4, y: 5, z: 6 }], @query.bind(4).to_a

    assert_equal [{ x: 1, y: 2, z: 3 }], @query.bind(1).to_a_hash
    assert_equal [{ x: 4, y: 5, z: 6 }], @query.bind(4).to_a_hash

    assert_equal [[1, 2, 3]], @query.bind(1).to_a_ary
    assert_equal [[4, 5, 6]], @query.bind(4).to_a_ary

    query = @db.prepare('select y from t')
    assert_equal [2, 5, 8], query.to_a_single_column
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
    @query.bind(1).each_ary { |r| buf << r }
    assert_equal [[1, 2, 3]], buf

    # each should reset the stmt
    buf = []
    @query.each_ary { |r| buf << r }
    assert_equal [[1, 2, 3]], buf

    query = @db.prepare('select * from t')
    buf = []
    query.each_ary { |r| buf << r }
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_ary_without_block
    query = @db.prepare('select * from t') 
    iter = query.each_ary
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_query_each_single_column
    query = @db.prepare('select x from t where x = ?')
    buf = []
    query.bind(1).each_single_column { |r| buf << r }
    assert_equal [1], buf

    # each should reset the stmt
    buf = []
    query.each_single_column { |r| buf << r }
    assert_equal [1], buf

    query = @db.prepare('select x from t')
    buf = []
    query.each_single_column { |r| buf << r }
    assert_equal [1, 4, 7], buf
  end

  def test_query_each_single_column_without_block
    query = @db.prepare('select x from t') 
    iter = query.each_single_column
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [1, 4, 7], buf
  end

  def test_query_with_invalid_sql
    assert_raises(Extralite::SQLError) { @db.prepare('blah').query }
  end

  def test_query_with_multiple_queries
    assert_raises(Extralite::Error) { @db.prepare('select 1; select 2').to_a }
  end

  def test_query_query_hash
    r = @query.query_hash(4)
    assert_equal [{x: 4, y: 5, z: 6}], r

    r = @query.query_hash(5)
    assert_equal [], r
  end

  def test_query_query_ary
    r = @query.query_ary(1)
    assert_equal [[1, 2, 3]], r

    r = @query.query_ary(2)
    assert_equal [], r
  end

  def test_query_query_single_row
    r = @query.query_single_row(4)
    assert_equal({ x: 4, y: 5, z: 6 }, r)

    r = @query.query_single_row(5)
    assert_nil r
  end

  def test_query_query_single_column
    query = 
    r = @db.prepare('select y from t').query_single_column
    assert_equal [2, 5, 8], r

    r = @db.prepare('select y from t where x = 2').query_single_column
    assert_equal [], r
end

  def test_query_query_single_value
    r = @db.prepare('select z from t order by Z desc limit 1').query_single_value
    assert_equal 9, r

    r = @db.prepare('select z from t where x = 2').query_single_value
    assert_nil r
  end

  def test_query_multiple_statements
    assert_raises(Extralite::Error) {
      @db.prepare("insert into t values ('a', 'b', 'c'); insert into t values ('d', 'e', 'f');").query
    }
  end

  def test_query_multiple_statements_with_bad_sql
    error = nil
    begin
      query =@db.prepare("insert into t values foo; insert into t values ('d', 'e', 'f');")
      query.query
    rescue => error
    end

    assert_kind_of Extralite::SQLError, error
    assert_equal 'near "foo": syntax error', error.message
  end

  def test_query_repeated_execution_missing_param
    r = @query.query_hash(4)
    assert_equal [{x: 4, y: 5, z: 6}], r

    r = @query.query_hash
    assert_equal [], r
  end

  def test_query_empty_sql
    assert_raises(Extralite::Error) { @db.prepare(' ') }

    r = @db.prepare('select 1 as foo;  ').query
    assert_equal [{ foo: 1 }], r
  end

  def test_query_parameter_binding_simple
    r = @db.prepare('select x, y, z from t where x = ?').query(1)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = ?').query(6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_query_parameter_binding_with_index
    r = @db.prepare('select x, y, z from t where x = ?2').query(0, 1)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = ?3').query(3, 4, 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_query_parameter_binding_with_name
    r = @db.prepare('select x, y, z from t where x = :x').query(x: 1, y: 2)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = :zzz').query('zzz' => 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r

    r = @db.prepare('select x, y, z from t where z = :bazzz').query(':bazzz' => 6)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_query_parameter_binding_with_index_key
    r = @db.prepare('select x, y, z from t where z = ?').query(1 => 3)
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where x = ?2').query(1 => 42, 2 => 4)
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_query_value_casting
    r = @db.prepare("select 'abc'").query_single_value
    assert_equal 'abc', r

    r = @db.prepare('select 123').query_single_value
    assert_equal 123, r

    r = @db.prepare('select 12.34').query_single_value
    assert_equal 12.34, r

    r = @db.prepare('select zeroblob(4)').query_single_value
    assert_equal "\x00\x00\x00\x00", r

    r = @db.prepare('select null').query_single_value
    assert_nil r
  end

  def test_query_columns
    r = @db.prepare("select 'abc' as a, 'def' as b").columns
    assert_equal [:a, :b], r
  end

  def test_query_close
    p = @db.prepare("select 'abc'")

    assert_equal false, p.closed?

    p.close
    assert_equal true, p.closed?

    p.close
    assert_equal true, p.closed?

    assert_raises(Extralite::Error) { p.query_single_value }
  end

  def test_query_execute_multi
    @db.query('create table foo (a, b, c)')
    assert_equal [], @db.query('select * from foo')

    records = [
      [1, '2', 3],
      ['4', 5, 6]
    ]

    p = @db.prepare('insert into foo values (?, ?, ?)')
    changes = p.execute_multi(records)

    assert_equal 2, changes
    assert_equal [
      { a: 1, b: '2', c: 3 },
      { a: '4', b: 5, c: 6 }
    ], @db.query('select * from foo')
  end  

  def test_query_status
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.query
    assert_equal 1, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.query
    assert_equal 2, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.query
    assert_equal 3, @query.status(Extralite::SQLITE_STMTSTATUS_RUN, true)
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
  end

  def test_query_status_after_close
    assert_equal 0, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.query
    assert_equal 1, @query.status(Extralite::SQLITE_STMTSTATUS_RUN)
    @query.close
    assert_raises(Extralite::Error) { @query.status(Extralite::SQLITE_STMTSTATUS_RUN) }
  end

  def test_query_after_db_close
    assert_equal [{ x: 4, y: 5, z: 6}], @query.query(4)
    @db.close
    assert_equal [{ x: 4, y: 5, z: 6}], @query.query(4)
  end
end
