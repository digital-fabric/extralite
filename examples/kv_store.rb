# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  gem 'polyphony'
  gem 'extralite', path: '.'
end

class KVStore
  def initialize(db)
    @db = db
    setup_kv_tables
    setup_queries
  end

  SETUP_SQL = <<~SQL
    create table if not exists kv (key text primary key, value, expires float);
    create index if not exists idx_kv_expires on kv (expires) where expires is not null;
  SQL

  def setup_kv_tables
    @db.execute SETUP_SQL
  end

  def setup_queries
    @q_get = @db.prepare_splat('select value from kv where key = ?')
    @q_set = @db.prepare('insert into kv (key, value) values($1, $2) on conflict (key) do update set value = $2')
  end

  def get(key)
    @q_get.bind(key).next
  end

  def set(key, value)
    @q_set.bind(key, value).execute
  end
end

db = Extralite::Database.new(':memory:')
kv = KVStore.new(db)

p get: kv.get('foo')
p set: kv.set('foo', 42)
p get: kv.get('foo')
p set: kv.set('foo', 43)
p get: kv.get('foo')

p db.query('select * from kv order by key')