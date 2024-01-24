# frozen_string_literal: true

# Run on Ruby 3.3 with YJIT enabled

require 'bundler/inline'
gemfile do
  gem 'polyphony'
  gem 'extralite', path: '.'
  gem 'benchmark-ips'
end

require 'benchmark/ips'
require 'polyphony'

DB_PATH = "/tmp/extralite_sqlite3_perf-#{Time.now.to_i}-#{rand(10000)}.db"
puts "DB_PATH = #{DB_PATH.inspect}"

$db1 = Extralite::Database.new(DB_PATH, gvl_release_threshold: -1)
$db2 = Extralite::Database.new(DB_PATH, gvl_release_threshold: 0)
$db3 = Extralite::Database.new(DB_PATH)

$snooze_count = 0
$db3.on_progress(25) { $snooze_count += 1; snooze }

def prepare_database(count)
  $db1.execute('create table if not exists foo ( a integer primary key, b text )')
  $db1.transaction do
    $db1.execute('delete from foo')
    rows = count.times.map { "hello#{rand(1000)}" }
    $db1.batch_execute('insert into foo (b) values (?)', rows)
  end
end

def extralite_run1(count)
  results = $db1.query('select * from foo')
  raise unless results.size == count
end

def extralite_run2(count)
  results = $db2.query('select * from foo')
  raise unless results.size == count
end

def extralite_run3(count)
  results = $db3.query('select * from foo')
  raise unless results.size == count
end

[10, 1000, 100000].each do |c|
  puts "Record count: #{c}"
  prepare_database(c)

  bm = Benchmark.ips do |x|
    x.config(:time => 3, :warmup => 1)

    x.report('GVL threshold -1') { extralite_run1(c) }
    x.report('GVL threshold  0') { extralite_run2(c) }
    $snooze_count  = 0
    x.report('on_progress 1000') { extralite_run3(c) }

    x.compare!
  end
  puts;
  bm.entries.each do |e|
    score = (e.ips * c).round.to_i
    if e.label == 'on_progress 1000'
      snooze_rate = ($snooze_count / e.seconds).to_i
      puts "#{e.label}: #{score} rows/s  snoozes: #{snooze_rate} i/s"
    else
      puts "#{e.label}: #{score} rows/s"
    end
  end
  puts;
end
