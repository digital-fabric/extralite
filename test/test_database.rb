# frozen_string_literal: true

require_relative 'helper'

class DatabaseTest < MiniTest::Test
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
    r = @db.query_single_value('select 42')
    assert_equal 42, r

    assert_equal @db, @db.close
    assert_equal true, @db.closed?

    assert_raises(Extralite::Error) { @db.query_single_value('select 42') }
  end
end

class ScenarioTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new('/tmp/extralite.db')
    @db.query('create table if not exists t (x,y,z)')
    @db.query('delete from t')
    @db.query('insert into t values (1, 2, 3)')
    @db.query('insert into t values (4, 5, 6)')
  end

  def test_concurrent_transactions
    done = false
    t = Thread.new do
      db = Extralite::Database.new('/tmp/extralite.db')
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
    result = @db.query_single_column('select x from t')
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

    result = @db.query_single_column('select x from t')
    assert_equal [1, 4, 7], result
  end
end

