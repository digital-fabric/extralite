# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'extralite', path: '..'
  gem 'sqlite3'
  gem 'benchmark-ips'
end

require 'benchmark/ips'
require 'fileutils'

DB_PATH = '/tmp/extralite_sqlite3_perf.db'

def prepare_database(count)
  FileUtils.rm(DB_PATH) rescue nil
  db = Extralite::Database.new(DB_PATH)
  db.query('create table foo ( a integer primary key, b text )')
  db.query('begin')
  count.times { db.query('insert into foo (b) values (?)', "hello#{rand(1000)}" )}
  db.query('commit')
end

def sqlite3_prepare
  db = SQLite3::Database.new(DB_PATH, :results_as_hash => true)
  db.prepare('select * from foo')
end

def sqlite3_run(stmt, count)
  # db = SQLite3::Database.new(DB_PATH, :results_as_hash => true)
  results = stmt.execute.to_a
  raise unless results.size == count
end

def extralite_prepare
  db = Extralite::Database.new(DB_PATH)
  db.prepare('select * from foo')
end

def extralite_run(query, count)
  # db = Extralite::Database.new(DB_PATH)
  results = query.to_a
  raise unless results.size == count
end

[10, 1000, 100000].each do |c|
  puts; puts; puts "Record count: #{c}"

  prepare_database(c)

  sqlite3_stmt = sqlite3_prepare
  extralite_stmt = extralite_prepare

  Benchmark.ips do |x|
    x.config(:time => 3, :warmup => 1)

    x.report("sqlite3")   { sqlite3_run(sqlite3_stmt, c) }
    x.report("extralite") { extralite_run(extralite_stmt, c) }

    x.compare!
  end
end
