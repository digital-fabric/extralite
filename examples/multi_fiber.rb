# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  gem 'polyphony'
  gem 'extralite', path: '.'
end

require 'extralite'
require 'polyphony'
require 'json'
require 'tempfile'

class Store
  def initialize(fn)
    @db = Extralite::Database.new(fn)
    @db.pragma(journal_mode: 'wal', 'synchronous': 1)
    @db.on_progress(20) { snooze }
    setup_tables
    setup_queries
  end

  SETUP_SQL = <<~SQL
    create table if not exists nodes (record text);
    create table if not exists states (stamp float, path text, value);

    create unique index if not exists idx_nodes_path on nodes (json_extract(record, '$.path'));
    create index if not exists idx_states_path_stamp on states (path, stamp);
  SQL

  def setup_tables
    @db.execute SETUP_SQL
  end

  NODE_INSERT_SQL = <<~SQL
    insert into nodes (record) values (?)
    on conflict do update set record = json_patch(record, excluded.record)
  SQL

  NODE_GET_SQL = <<~SQL
    select record from nodes
    where json_extract(record, '$.path') = ?
    limit 1
  SQL

  STATES_INSERT_SQL = <<~SQL
    insert into states (stamp, path, value)
    values (?, ?, ?)
  SQL

  def setup_queries
    @q_node_get = @db.prepare(NODE_GET_SQL)
    @q_node_update = @db.prepare(NODE_INSERT_SQL)
    @q_states_insert = @db.prepare(STATES_INSERT_SQL)
  end

  def update_state(path, state)
    node_values = {
      path: path,
      state: state
    }
    @db.transaction do
      @q_node_update.execute(node_values.to_json)
      @q_states_insert.execute(state[:stamp], path, state[:value])
    end
  end

  def node_get(path)
    row = @q_node_get.bind(path).next_single_column
    row ? JSON.load(row) : nil
  end

  def all_nodes
    @db.query_single_column("select record from nodes order by json_extract(record, '$.path')")
  end

  def all_states
    @db.query("select * from nodes order by json_extract(record, '$.path')")
  end
end

fn = Tempfile.new('multi_fiber').path
store = Store.new(fn)

p node_get1: store.node_get('/r1')
store.update_state('/r1', { stamp: Time.now.to_f, value: 42 })
p node_get2: store.node_get('/r1')
store.update_state('/r1', { stamp: Time.now.to_f, value: 43 })
p node_get3: store.node_get('/r1')
p all: store.all_nodes