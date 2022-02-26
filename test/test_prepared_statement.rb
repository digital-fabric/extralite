# frozen_string_literal: true

require_relative 'helper'

class PreparedStatementTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.prepare('create table if not exists t (x,y,z)').query
    @db.prepare('delete from t').query
    @db.prepare('insert into t values (1, 2, 3)').query
    @db.prepare('insert into t values (4, 5, 6)').query

    @stmt = @db.prepare('select * from t where x = ?')
  end

  def test_prepared_statement_props
    assert_kind_of Extralite::PreparedStatement, @stmt
    assert_equal @db, @stmt.Database
    assert_equal 'select * from t where x = ?', @stmt.sql
  end

  def test_prepared_statement_with_missing_argument
    assert_raises(Extralite::Error) { @stmt.query }
  end

  def test_prepared_statement_query
    assert_equal [{ x: 1, y: 2, z: 3 }], @stmt.query(1)

    buf = []
    @stmt.query(1) { |r| buf << r }
    assert_equal [{ x: 1, y: 2, z: 3 }], buf
  end

  def test_prepared_statement_with_invalid_sql
    stmt = @db.prepare('blah')
    assert_raises(Extralite::SQLError) { stmt.query }
  end

  def test_prepared_statement_query_hash
    r = @stmt.query_hash(4)
    assert_equal [{x: 4, y: 5, z: 6}], r

    r = @stmt.query_hash(5)
    assert_equal [], r
  end

  def test_prepared_statement_query_ary
    r = @stmt.query_ary(1)
    assert_equal [[1, 2, 3]], r

    r = @stmt.query_ary(2)
    assert_equal [], r
  end

  def test_prepared_statement_query_single_row
    r = @stmt.query_single_row(4)
    assert_equal({ x: 4, y: 5, z: 6 }, r)

    r = @stmt.query_single_row(5)
    assert_nil r
  end

  def test_prepared_statement_query_single_column
    stmt = 
    r = @db.prepare('select y from t').query_single_column
    assert_equal [2, 5], r

    r = @db.prepare('select y from t where x = 2').query_single_column
    assert_equal [], r
end

  def test_prepared_statement_query_single_value
    r = @db.prepare('select z from t order by Z desc limit 1').query_single_value
    assert_equal 6, r

    r = @db.prepare('select z from t where x = 2').query_single_value
    assert_nil r
  end

  def test_prepared_statement_multiple_statements
    stmt = @db.prepare("insert into t values ('a', 'b', 'c'); insert into t values ('d', 'e', 'f');")
    assert_raises(Extralite::Error) { stmt.query }
  end

  def test_prepared_statement_multiple_statements_with_bad_sql
    error = nil
    begin
      stmt =@db.prepare("insert into t values foo; insert into t values ('d', 'e', 'f');")
      stmt.query
    rescue => error
    end

    assert_kind_of Extralite::SQLError, error
    assert_equal 'near "foo": syntax error', error.message
  end

  def test_prepared_statement_empty_sql
    r = @db.prepare(' ').query
    assert_nil r

    r = @db.prepare('select 1 as foo;  ').query
    assert_equal [{ foo: 1 }], r
  end

  def test_prepared_statement_parameter_binding_simple
    r = @db.prepare('select x, y, z from t where x = ?', 1).query
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = ?', 6).query
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_prepared_statement_parameter_binding_with_index
    r = @db.prepare('select x, y, z from t where x = ?2', 0, 1).query
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = ?3', 3, 4, 6).query
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_prepared_statement_parameter_binding_with_name
    r = @db.prepare('select x, y, z from t where x = :x', x: 1, y: 2).query
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where z = :zzz', 'zzz' => 6).query
    assert_equal [{ x: 4, y: 5, z: 6 }], r

    r = @db.prepare('select x, y, z from t where z = :bazzz', ':bazzz' => 6).query
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_prepared_statement_parameter_binding_with_index_key
    r = @db.prepare('select x, y, z from t where z = ?', 1 => 3).query
    assert_equal [{ x: 1, y: 2, z: 3 }], r

    r = @db.prepare('select x, y, z from t where x = ?2', 1 => 42, 2 => 4).query
    assert_equal [{ x: 4, y: 5, z: 6 }], r
  end

  def test_prepared_statement_value_casting
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
end
