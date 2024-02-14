# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  gem 'polyphony'
  gem 'extralite', path: '.'
  gem 'benchmark-ips'
end

require 'tempfile'

class PubSub
  def initialize(db)
    @db = db
  end

  def attach
    @db.execute('insert into subscribers (stamp) values (?)', Time.now.to_i)
    @id = @db.last_insert_rowid
  end

  def detach
    @db.execute('delete from subscribers where id = ?', @id)
  end

  def subscribe(*topics)
    @db.transaction do
      topics.each do |t|
        @db.execute('insert into subscriber_topics (subscriber_id, topic) values (?, ?)', @id, t)
      end
    end
  end

  def unsubscribe(*topics)
    @db.transaction do
      topics.each do |t|
        @db.execute('delete from subscriber_topics where subscriber_id = ? and topic = ?', @id, t)
      end
    end
  end

  def get_messages(&block)
    #   @db.execute('update subscribers set stamp = ? where id = ?', Time.now.to_i, @id)
      @db.query_splat('delete from messages where subscriber_id = ? returning topic, message', @id, &block)
  end

  SCHEMA = <<~SQL
    PRAGMA foreign_keys = ON;

    create table if not exists subscribers (
      id integer primary key,
      stamp float
    );
    create index if not exists idx_subscribers_stamp on subscribers (stamp);

    create table if not exists subscriber_topics (
      subscriber_id integer references subscribers(id) on delete cascade,
      topic text
    );

    create table if not exists messages(
      subscriber_id integer,
      topic text,
      message text, 
      foreign key (subscriber_id, topic)
        references subscriber_topics(subscriber_id, topic)
        on delete cascade
    );
    create index if not exists idx_messages_subscriber_id_topic on messages (subscriber_id, topic);
  SQL

  def setup
    @db.transaction do
      @db.execute(SCHEMA)
    end
  end

  def publish(topic, message)
    @db.execute("
      with subscribed as (
        select subscriber_id
        from subscriber_topics
        where topic = $1
      )
      insert into messages
      select subscriber_id, $1, $2
      from subscribed
    ", topic, message)
  end

  def prune_subscribers
    @db.execute('delete from subscribers where stamp < ?', Time.now.to_i - 3600)
  end
end

fn = Tempfile.new('pubsub_store').path
p fn: fn
db1 = Extralite::Database.new(fn)
db1.pragma(journal_mode: :wal, synchronous: 1)
db2 = Extralite::Database.new(fn)
db2.pragma(journal_mode: :wal, synchronous: 1)
db3 = Extralite::Database.new(fn)
db3.pragma(journal_mode: :wal, synchronous: 1)

db1.on_progress(1000) { |b| b ? sleep(0.0001) : snooze }
db2.on_progress(1000) { |b| b ? sleep(0.0001) : snooze }
db3.on_progress(1000) { |b| b ? sleep(0.0001) : snooze }

producer = PubSub.new(db1)
producer.setup

consumer1 = PubSub.new(db2)
consumer1.setup
consumer1.attach
consumer1.subscribe('foo', 'bar')

consumer2 = PubSub.new(db3)
consumer2.setup
consumer2.attach
consumer2.subscribe('foo', 'baz')


# producer.publish('foo', 'foo1')
# producer.publish('bar', 'bar1')
# producer.publish('baz', 'baz1')

# puts "- get messages"
# consumer1.get_messages { |t, m| p consumer: 1, topic: t, message: m }
# consumer2.get_messages { |t, m| p consumer: 2, topic: t, message: m }

# puts "- get messages again"
# consumer1.get_messages { |t, m| p consumer: 1, topic: t, message: m }
# consumer2.get_messages { |t, m| p consumer: 2, topic: t, message: m }

topics = %w{bar baz}

publish_count = 0
f1 = spin do
  while true
    message = "message#{rand(1000)}"
    producer.publish(topics.sample, message)

    publish_count += 1
    snooze
  end
end

receive_count = 0
f2 = spin do
  while true
    consumer1.get_messages { |t, m| receive_count += 1 }
    sleep 0.1
  end
end

f3 = spin do
  while true
    consumer2.get_messages { |t, m| receive_count += 1 }
    sleep 0.1
  end
end

db4 = Extralite::Database.new(fn)
db4.pragma(journal_mode: :wal, synchronous: 1)
db4.on_progress(1000) { |busy| busy ? sleep(0.05) : snooze }

last_t = Time.now
last_publish_count = 0
last_receive_count = 0
while true
  sleep 1
  now = Time.now
  elapsed = now - last_t
  d_publish = publish_count - last_publish_count
  d_receive = receive_count - last_receive_count
  pending = db4.query_single_splat('select count(*) from messages')
  puts "#{Time.now} publish: #{d_publish/elapsed}/s receive: #{d_receive/elapsed}/s pending: #{pending}"
  last_t = now
  last_publish_count = publish_count
  last_receive_count = receive_count
end

# bm = Benchmark.ips do |x|
#   x.config(:time => 10, :warmup => 2)

#   x.report("pubsub") do
#     db.transaction { 10.times { |i| producer.publish('foo', "foo#{i}") } }
#     consumer1.get_messages
#     consumer2.get_messages
#   end

#   x.compare!
# end
