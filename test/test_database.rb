# frozen_string_literal: true

require_relative 'helper'

class DatabaseTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new('/tmp/extralite.db')
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

  def test_query_hash
    r = @db.query_hash('select * from t')
    assert_equal [{x: 1, y: 2, z: 3}, {x: 4, y: 5, z: 6}], r

    r = @db.query_hash('select * from t where x = 2')
    assert_equal [], r
  end

  def test_query_ary
    r = @db.query_ary('select * from t')
    assert_equal [[1, 2, 3], [4, 5, 6]], r

    r = @db.query_ary('select * from t where x = 2')
    assert_equal [], r
  end

  def test_query_single_row
    r = @db.query_single_row('select * from t order by x desc limit 1')
    assert_equal({ x: 4, y: 5, z: 6 }, r)

    r = @db.query_single_row('select * from t where x = 2')
    assert_nil r
  end

  def test_query_single_column
    r = @db.query_single_column('select y from t')
    assert_equal [2, 5], r
  
    r = @db.query_single_column('select y from t where x = 2')
    assert_equal [], r
end

  def test_query_single_value
    r = @db.query_single_value('select z from t order by Z desc limit 1')
    assert_equal 6, r

    r = @db.query_single_value('select z from t where x = 2')
    assert_nil r
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

    assert_equal [1, 4, 'a', 'd'], @db.query_single_column('select x from t order by x')
  end

  def test_empty_sql
    r = @db.query(' ')
    assert_nil r

    r = @db.query('select 1 as foo;  ')
    assert_equal [{ foo: 1 }], r
  end
end
