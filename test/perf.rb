# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'sqlite3'
  gem 'extralite', path: '..'
  gem 'benchmark-ips'
end

require 'benchmark/ips'
require 'fileutils'

DB_PATH = '/tmp/extralite_sqlite3_perf.db'
COUNT = 10000

def prepare_database
  FileUtils.rm(DB_PATH) rescue nil
  db = Extralite::Database.new(DB_PATH)
  db.query('create table foo ( a integer primary key, b text )')
  db.query('begin')
  COUNT.times { db.query('insert into foo (b) values (?)', "hello#{rand(1000)}" )}
  db.query('commit')
end

def sqlite3_run
  db = SQLite3::Database.new(DB_PATH, :results_as_hash => true)
  results = db.execute('select * from foo')
  raise unless results.size == COUNT
end

def extralite_run
  db = Extralite::Database.new(DB_PATH)
  results = db.query('select * from foo')
  raise unless results.size == COUNT
end

prepare_database

Benchmark.ips do |x|
  x.config(:time => 3, :warmup => 1)

  x.report("sqlite3") { sqlite3_run }
  x.report("extralite") { extralite_run }

  x.compare!
end
