# frozen_string_literal: true

# Run on Ruby 3.3 with YJIT enabled

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'extralite', path: '..'
  gem 'sqlite3'
  gem 'benchmark-ips'
end

require 'benchmark/ips'
require 'fileutils'

DB_PATH = "/tmp/extralite_sqlite3_perf-#{Time.now.to_i}-#{rand(10000)}.db"
puts "DB_PATH = #{DB_PATH.inspect}"

$sqlite3_db = SQLite3::Database.new(DB_PATH, results_as_hash: true)
$extralite_db = Extralite::Database.new(DB_PATH, gvl_release_threshold: -1)

def prepare_database(count)
  $extralite_db.query('create table if not exists foo ( a integer primary key, b text )')
  $extralite_db.query('delete from foo')
  $extralite_db.query('begin')
  count.times { $extralite_db.query('insert into foo (b) values (?)', "hello#{rand(1000)}" )}
  $extralite_db.query('commit')
end

def sqlite3_run(count)
  results = $sqlite3_db.execute('select * from foo')
  raise unless results.size == count
end

def extralite_run(count)
  results = $extralite_db.query('select * from foo')
  raise unless results.size == count
end

[10, 1000, 100000].each do |c|
  puts "Record count: #{c}"
  prepare_database(c)

  bm = Benchmark.ips do |x|
    x.config(:time => 5, :warmup => 2)

    x.report("sqlite3") { sqlite3_run(c) }
    x.report("extralite") { extralite_run(c) }

    x.compare!
  end
  puts;
  bm.entries.each { |e| puts "#{e.label}: #{(e.ips * c).round.to_i} rows/s" }
  puts;
end
