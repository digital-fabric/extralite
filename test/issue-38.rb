# frozen_string_literal: true

require "sqlite3"
require "extralite"
require "benchmark"

# Setup

File.delete("benchmark.sqlite3") if File.exist?("benchmark.sqlite3")

POOL_SIZE = 10

EXTRALITE_CONNECTIONS = POOL_SIZE.times.map do
  db = Extralite::Database.new("benchmark.sqlite3")
  db.execute("PRAGMA journal_mode = WAL")
  db.execute("PRAGMA synchronous = NORMAL")
  db.execute("PRAGMA journal_size_limit = 64000000")
  db.execute("PRAGMA mmap_size = 128000000")
  db.execute("PRAGMA cache_size = 2000")
  db.execute("PRAGMA busy_timeout = 5000")
  db
end

SQLITE3_CONNECTIONS = POOL_SIZE.times.map do
  db = SQLite3::Database.new("benchmark.sqlite3")
  db.execute("PRAGMA journal_mode = WAL")
  db.execute("PRAGMA synchronous = NORMAL")
  db.execute("PRAGMA journal_size_limit = 64000000")
  db.execute("PRAGMA mmap_size = 128000000")
  db.execute("PRAGMA cache_size = 2000")
  db.execute("PRAGMA busy_timeout = 5000")
  db
end

EXTRALITE_CONNECTIONS[0].execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT, updated_at TEXT) STRICT")
insert_statement = EXTRALITE_CONNECTIONS[0].prepare("INSERT INTO users (name, created_at, updated_at) VALUES (?, ?, ?)")
1000.times do
  insert_statement.execute("John Doe", Time.now.iso8601, Time.now.iso8601)
end

# Benchmark variations

THREAD_COUNTS = [1, 2, 4, 8]
LIMITS = [1000]#[10, 100, 1000]
CLIENTS = %w[extralite sqlite3]

# Benchmark

GC.disable
Benchmark.bm do |x|
  LIMITS.each do |limit|
    THREAD_COUNTS.each do |thread_count|
      CLIENTS.each do |client|
        GC.start

        x.report("#{client.rjust('extralite'.length)} - limit: #{limit}, threads: #{thread_count}") do
          threads = thread_count.times.map do |thread_number|
            Thread.new do
              start = Time.now
              if client == "extralite"
                1_000.times do
                  records = EXTRALITE_CONNECTIONS[thread_number].query_ary("SELECT * FROM users LIMIT #{limit}")
                  raise "Expected #{limit} but got #{length}" unless records.length == limit
                end
              else
                1_000.times do
                  records = SQLITE3_CONNECTIONS[thread_number].query("SELECT * FROM users LIMIT #{limit}").entries
                  raise "Expected #{limit} but got #{length}" unless records.length == limit
                end
              end
            end
          end
          threads.each(&:join)
        end
      end
      puts
    end
  end
end
GC.enable