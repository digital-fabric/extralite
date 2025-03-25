# frozen_string_literal: true

require_relative 'helper'
require 'date'
require 'json'

class DatabaseTraceTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')

    @trace_buf = []
    @db.trace { |sql, *args| @trace_buf << { sql: sql, args: args } }
  end

  def test_trace_database_query
    @db.query('select 1')
    @db.query('select ?', 2)
    @db.query('select ?, ?, ?', 3, 4, 5)
    @db.query('select :x, :y, :z', x: 6, y: 7)

    assert_equal [
      { sql: 'select 1', args: [] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?, ?, ?', args: [3, 4, 5] },
      { sql: 'select :x, :y, :z', args: [{ x: 6, y: 7 }] }
    ], @trace_buf
  end

  def test_trace_database_execute
    @db.execute('select 1')
    @db.execute('select ?', 2)
    @db.execute('select ?, ?, ?', 3, 4, 5)
    @db.execute('select :x, :y, :z', x: 6, y: 7)

    assert_equal [
      { sql: 'select 1', args: [] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?, ?, ?', args: [3, 4, 5] },
      { sql: 'select :x, :y, :z', args: [{ x: 6, y: 7 }] }
    ], @trace_buf
  end

  def test_trace_database_query_xxx
    @db.query_single('select 1')
    @db.query_array('select ?', 2)
    @db.query_splat('select ?, ?, ?', 3, 4, 5)
    @db.query_single_array('select :x, :y, :z', x: 6, y: 7)

    assert_equal [
      { sql: 'select 1', args: [] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?, ?, ?', args: [3, 4, 5] },
      { sql: 'select :x, :y, :z', args: [{ x: 6, y: 7 }] }
    ], @trace_buf
  end

  def test_trace_database_batch_execute
    @db.batch_execute('select ?', [1, 2, 3])
    @db.batch_execute('select ?, ?', [[44, 55], [66, 77]])
    @db.batch_execute('select :x, :y', [{ x: 5, y: 6 }, { x: 7, y: 8 }])

    assert_equal [
      { sql: 'select ?', args: [1] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?', args: [3] },
      { sql: 'select ?, ?', args: [44, 55] },
      { sql: 'select ?, ?', args: [66, 77] },
      { sql: 'select :x, :y', args: [{ x: 5, y: 6 }] },
      { sql: 'select :x, :y', args: [{ x: 7, y: 8 }] }
    ], @trace_buf
  end

  def test_trace_database_batch_query
    @db.batch_query('select ?', [1, 2, 3])
    @db.batch_query('select ?, ?', [[44, 55], [66, 77]])
    @db.batch_query('select :x, :y', [{ x: 5, y: 6 }, { x: 7, y: 8 }])

    assert_equal [
      { sql: 'select ?', args: [1] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?', args: [3] },
      { sql: 'select ?, ?', args: [44, 55] },
      { sql: 'select ?, ?', args: [66, 77] },
      { sql: 'select :x, :y', args: [{ x: 5, y: 6 }] },
      { sql: 'select :x, :y', args: [{ x: 7, y: 8 }] }
    ], @trace_buf
  end

  def test_trace_database_batch_query_with_enumerable
    @db.batch_query('select ?', 1..3)
    
    assert_equal [
      { sql: 'select ?', args: [1] },
      { sql: 'select ?', args: [2] },
      { sql: 'select ?', args: [3] }
    ], @trace_buf
  end

  def test_trace_database_batch_query_with_proc
    values = [4, 5, 6]

    @db.batch_query('select ?', -> { values.shift })
    
    assert_equal [
      { sql: 'select ?', args: [4] },
      { sql: 'select ?', args: [5] },
      { sql: 'select ?', args: [6] }
    ], @trace_buf
  end
end

class QueryTraceTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')

    @query = @db.prepare('select ?')

    @trace_buf = []
    @db.trace { |sql, *args| @trace_buf << { sql: sql, args: args } }
  end

  def test_trace_query_iteration
    assert_equal [], @trace_buf

    @query.bind(42)
    assert_equal [], @trace_buf

    @query.to_a
    assert_equal [
      { sql: 'select ?', args: [42] },
    ], @trace_buf

    @query.to_a
    assert_equal [
      { sql: 'select ?', args: [42] },
      { sql: 'select ?', args: [42] }
    ], @trace_buf

    @query.bind(44)
    assert_equal [
      { sql: 'select ?', args: [42] },
      { sql: 'select ?', args: [42] }
    ], @trace_buf

    @query.reset
    assert_equal [
      { sql: 'select ?', args: [42] },
      { sql: 'select ?', args: [42] },
      { sql: 'select ?', args: [44] }
    ], @trace_buf

    @trace_buf = []
    @query.bind(46)
    assert_equal [], @trace_buf

    @query.each { }
    assert_equal [
      { sql: 'select ?', args: [46] }
    ], @trace_buf

    @trace_buf = []
    @query.bind(48)
    assert_equal [], @trace_buf

    e = @query.each
    e.to_a
    assert_equal [
      { sql: 'select ?', args: [48] }
    ], @trace_buf

    e.map {}
    assert_equal [
      { sql: 'select ?', args: [48] },
      { sql: 'select ?', args: [48] }
    ], @trace_buf
  end

  def test_trace_query_batch_execute
    assert_equal [], @trace_buf

    @query.batch_execute([42, 43, 44])
    assert_equal [
      { sql: 'select ?', args: [42] },
      { sql: 'select ?', args: [43] },
      { sql: 'select ?', args: [44] },
    ], @trace_buf
    
  end
end
