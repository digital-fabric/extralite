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

def sqlite3_run(count)
  db = SQLite3::Database.new(DB_PATH, :results_as_hash => true)
  results = db.execute('select * from foo')
  raise unless results.size == count
end

def extralite_run(count)
  db = Extralite::Database.new(DB_PATH)
  results = db.query('select * from foo')
  raise unless results.size == count
end

[10, 1000, 100000].each do |c|
  puts; puts; puts "Record count: #{c}"

  prepare_database(c)

  Benchmark.ips do |x|
    x.config(:time => 3, :warmup => 1)

    x.report("sqlite3") { sqlite3_run(c) }
    x.report("extralite") { extralite_run(c) }

    x.compare!
  end
end
