# frozen_string_literal: true

require_relative 'helper'

# Runs each API with each kind of failure (bind error, step error, exception
# in block, break), followed by a rerun, a nested call, close or GC, and
# compares the result with a database without stmt cache.
class StmtCacheMatrixTest < Minitest::Test
  def make_db(**opts)
    db = Extralite::Database.new(':memory:', **opts)
    db.execute('create table t (id integer primary key, x integer check (x >= 0))')
    db.execute('insert into t values (1, 1), (2, 2), (3, 3)')
    db
  end

  BindError  = :bind_error
  StepError  = :step_error
  BlockError = :block_error
  Break      = :break

  # Each API entry gets a db, a failure kind and an optional block, and returns
  # the result of the call. The SQL for each API is chosen so that a failure of
  # the given kind can happen.
  READ_SQL = 'select id, json(?) as j from t where id > ?'
  WRITE_SQL = 'insert into t values (?, ?) returning id'

  def self.params_for(kind, i = 0)
    case kind
    when BindError then [Object.new, 0]
    when StepError then ['{', 0]
    else ['1', i]
    end
  end

  def self.write_params_for(kind, i = 0)
    case kind
    when BindError then [100 + i, Object.new]
    when StepError then [1, 0]
    else [100 + i, 0]
    end
  end

  READ_APIS = {
    query:              ->(db, args, &b) { db.query(READ_SQL, *args, &b) },
    query_array:        ->(db, args, &b) { db.query_array(READ_SQL, *args, &b) },
    query_splat:        ->(db, args, &b) { db.query_splat(READ_SQL, *args, &b) },
    query_single:       ->(db, args, &b) { b ? db.query(READ_SQL, *args, &b) : db.query_single(READ_SQL, *args) },
    query_single_splat: ->(db, args, &b) { b ? db.query_splat(READ_SQL, *args, &b) : db.query_single_splat(READ_SQL, *args) },
    batch_query:        ->(db, args, &b) { db.batch_query(READ_SQL, [args, args], &b) },
    batch_query_array:  ->(db, args, &b) { db.batch_query_array(READ_SQL, [args, args], &b) },
    batch_query_splat:  ->(db, args, &b) { db.batch_query_splat(READ_SQL, [args, args], &b) },
    prepare_next:       ->(db, args, &b) { q = db.prepare(READ_SQL, *args); b ? q.each(&b) : [q.next, q.next] },
    prepare_to_a:       ->(db, args, &b) { q = db.prepare_array(READ_SQL, *args); b ? q.each(&b) : q.to_a },
    iterator:           ->(db, args, &b) { it = db.prepare_splat(READ_SQL, *args).each; b ? it.each(&b) : it.to_a },
  }

  WRITE_APIS = {
    execute:            ->(db, args, &b) { b ? db.query(WRITE_SQL, *args, &b) : db.execute(WRITE_SQL, *args) },
    batch_execute:      ->(db, args, &b) { b ? db.batch_query(WRITE_SQL, [args], &b) : db.batch_execute(WRITE_SQL, [args]) },
    query_returning:    ->(db, args, &b) { db.query(WRITE_SQL, *args, &b) },
    prepare_execute:    ->(db, args, &b) { q = db.prepare(WRITE_SQL); b ? q.bind(*args).each(&b) : q.execute(*args) },
  }

  FAILURES = [BindError, StepError, BlockError, Break]
  FOLLOW_UPS = [:rerun, :nested, :close, :gc]

  def run_failure(db, api, kind, args)
    case kind
    when BindError, StepError
      api.(db, args)
    when BlockError
      api.(db, args) { raise 'foo' }
    when Break
      api.(db, args) { break :broke }
    end
  rescue Extralite::Error, RuntimeError => e
    e.class
  end

  def run_follow_up(db, api, follow_up, params_for)
    case follow_up
    when :rerun
      [api.(db, params_for.(1)), api.(db, params_for.(2))]
    when :nested
      rows = []
      api.(db, params_for.(3)) { |*r| rows << [r, api.(db, params_for.(4))] }
      rows << api.(db, params_for.(5))
      rows
    when :close
      db.close
      db.closed?
    when :gc
      GC.start
      api.(db, params_for.(6))
    end
  end

  def run_scenario(stmt_cache, api, kind, follow_up, params_for)
    db = make_db(stmt_cache: stmt_cache)
    failure = run_failure(db, api, kind, params_for.(kind))
    # make sure failed calls don't leave references behind
    GC.start if follow_up == :gc
    result = run_follow_up(db, api, follow_up, ->(i) { params_for.(:ok, i) })
    rows = db.closed? ? nil : db.query_array('select * from t order by id')
    [failure, result, rows]
  ensure
    db.close if db && !db.closed?
  end

  { read: [READ_APIS, :params_for], write: [WRITE_APIS, :write_params_for] }.each do |group, (apis, params_method)|
    apis.each do |api_name, api|
      FAILURES.each do |kind|
        FOLLOW_UPS.each do |follow_up|
          define_method(:"test_matrix_#{api_name}_#{kind}_#{follow_up}") do
            params_for = ->(k, i = 0) { self.class.send(params_method, k, i) }
            expected = run_scenario(false, api, kind, follow_up, params_for)
            actual = run_scenario(true, api, kind, follow_up, params_for)
            assert_equal expected, actual
          end
        end
      end
    end
  end
end
