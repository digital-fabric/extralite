# frozen_string_literal: true

require './lib/extralite'
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
    @db.transaction(:deferred) do
      results = @db.query_ary('select topic, message from messages where subscriber_id = ?', @id)
      return [] if results.empty?

      @db.execute('delete from messages where subscriber_id = ?', @id)
      results
    end

      # messages = @db.query_ary('delete from messages where subscriber_id = ? returning topic, message', @id)
      # if block
      #   messages.each(&block)
      #   nil
      # else
      #   messages
      # end
  rescue Extralite::BusyError
    p busy: :get_message
    block_given? ? nil : []
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
    # @db.transaction do
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
    # end
  rescue Extralite::BusyError
    p busy: :publish
    retry
  end

  def prune_subscribers
    @db.execute('delete from subscribers where stamp < ?', Time.now.to_i - 3600)
  end
end

fn = Tempfile.new('pubsub_store').path
p fn
db1 = Extralite::Database.new(fn)
db1.busy_timeout = 0.1
db1.pragma(journal_mode: :wal, synchronous: 1)
db1.gvl_release_threshold = -1
db2 = Extralite::Database.new(fn)
db2.pragma(journal_mode: :wal, synchronous: 1)
db2.busy_timeout = 0.1
db2.gvl_release_threshold = -1
db3 = Extralite::Database.new(fn)
db3.pragma(journal_mode: :wal, synchronous: 1)
db3.busy_timeout = 0.1
db3.gvl_release_threshold = -1

# db1.on_progress(100) { Thread.pass }
# db2.on_progress(100) { Thread.pass }
# db3.on_progress(100) { Thread.pass }

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
t1 = Thread.new do
  while true
    message = "message#{rand(1000)}"
    producer.publish(topics.sample, message)

    publish_count += 1
    Thread.pass
  end
end

receive_count = 0

t2 = Thread.new do
  while true
    consumer1.get_messages { |t, m| receive_count += 1 }
    sleep(0.1)
  end
end

t3 = Thread.new do
  while true
    consumer2.get_messages { |t, m| receive_count += 1 }
    sleep(0.1)
  end
end

db4 = Extralite::Database.new(fn)
db4.pragma(journal_mode: :wal, synchronous: 1)
db4.gvl_release_threshold = -1
db4.busy_timeout = 3

last_t = Time.now
last_publish_count = 0
last_receive_count = 0
while true
  sleep 1
  now = Time.now
  elapsed = now - last_t
  d_publish = publish_count - last_publish_count
  d_receive = receive_count - last_receive_count

  count = db4.query_single_argv('select count(*) from messages')
  puts "#{Time.now} publish: #{d_publish/elapsed}/s receive: #{d_receive/elapsed}/s pending: #{count}"
  last_t = now
  last_publish_count = publish_count
  last_receive_count = receive_count
end
