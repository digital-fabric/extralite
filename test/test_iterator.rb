# frozen_string_literal: true

require_relative 'helper'

class IteratorTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.query('create table if not exists t (x,y,z)')
    @db.query('delete from t')
    @db.query('insert into t values (1, 2, 3)')
    @db.query('insert into t values (4, 5, 6)')
    @db.query('insert into t values (7, 8, 9)')

    @query = @db.prepare('select * from t')
  end

  def test_iterator_each_idempotency
    iter = @query.each
    assert_equal iter, iter.each
    assert_equal iter, iter.each.each
  end

  def test_iterator_hash
    iter = @query.each
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [{x: 1, y: 2, z: 3},{ x: 4, y: 5, z: 6 }, { x: 7, y: 8, z: 9 }], buf
  end

  def test_iterator_ary
    iter = @query.each_ary
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [[1, 2, 3], [4, 5, 6], [7, 8, 9]], buf
  end

  def test_iterator_single_column
    query = @db.prepare('select x from t')
    iter = query.each_single_column
    assert_kind_of Extralite::Iterator, iter

    buf = []
    v = iter.each { |r| buf << r }
    assert_equal iter, v
    assert_equal [1, 4, 7], buf
  end

  def test_iterator_next
    iter = @query.each
    assert_equal({x: 1, y: 2, z: 3}, iter.next)
    assert_equal({x: 4, y: 5, z: 6}, iter.next)
    assert_equal({x: 7, y: 8, z: 9}, iter.next)
    assert_nil iter.next
    assert_nil iter.next

    iter = @query.reset.each_ary
    assert_equal([1, 2, 3], iter.next)
    assert_equal([4, 5, 6], iter.next)
    assert_equal([7, 8, 9], iter.next)
    assert_nil iter.next
    assert_nil iter.next

    iter = @db.prepare('select y from t').each_single_column
    assert_equal(2, iter.next)
    assert_equal(5, iter.next)
    assert_equal(8, iter.next)
    assert_nil iter.next
    assert_nil iter.next
  end

  def test_iterator_enumerable_methods
    mapped = @query.each.map { |row| row[:x] * 10 }
    assert_equal [10, 40, 70], mapped

    mapped = @query.each_ary.map { |row| row[1] * 10 }
    assert_equal [20, 50, 80], mapped

    query = @db.prepare('select z from t')
    mapped = query.each_single_column.map { |v| v * 10 }
    assert_equal [30, 60, 90], mapped
  end
end
