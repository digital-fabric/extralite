# frozen_string_literal: true

require "./lib/extralite"
require "benchmark"
require "tempfile"
require "fileutils"

p sqlite_version: Extralite.sqlite3_version

N = (ENV['N'] || 1000).to_i
p N: N

fn1 = '/tmp/db1'
fn2 = '/tmp/db2'

FileUtils.rm(fn1) rescue nil
FileUtils.rm(fn2) rescue nil

p fn1: fn1
p fn2: fn2

db1 = Extralite::Database.new fn1
db1.execute "pragma journal_mode = wal;"
db1.transaction do
  db1.execute "create table t1 ( a integer primary key, b text );"
  values = N.times.map { |i| "#{i}-#{rand(1000)}" }
  db1.execute_multi "insert into t1 ( b ) values ( ? );", values

  p count: db1.query_single_value("select count(*) from t1")
  p some_rows: db1.query("select * from t1 limit 5")
end

db2 = Extralite::Database.new fn2
db2.execute "pragma journal_mode = wal;"
db2.execute "attach '#{fn1}' as db1;"
db2.execute "create table t2 ( a integer primary key, b text );"

p main_tables: db2.tables
p db1_tables: db2.tables('db1')

overall = Benchmark.realtime do
  t1 = Thread.new do
    time1 = Benchmark.realtime do
      db2.execute "create unique index db1.t1_b_unique on t1 (b);"
    end
    p({ indexing: time1 })
  end

  t2 = Thread.new do
    time2 = Benchmark.realtime do
      (N / 10000).times do |i|
        values = 10000.times.map { |i| "#{i}-#{rand(1000)}" }
        db2.transaction do
          db2.execute_multi "insert into main.t2 ( b ) values ( ? );", values
        end
      end
    end
    p({ inserting: time2 })
    p count_t2: db2.query_single_value("select count(*) from main.t2")
    p some_rows_t2: db2.query("select * from main.t2 limit 5")
    end

  t1.join
  t2.join
end

p({ overall: overall })

db1.close
db2.close
