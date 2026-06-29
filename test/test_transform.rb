# frozen_string_literal: true

require_relative 'helper'
require 'date'
require 'json'

class TransformOneToOneTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.pragma('foreign_keys' => 1)
    @db.execute <<~SQL
      create table posts (
        id integer primary key,
        title text,
        content text,
        author_id integer references authors(id)
          on delete cascade
      );
      create table authors (
        id integer primary key,
        name text
      );

      insert into authors (name) values ('Foo');
      insert into authors (name) values ('Bar');
      insert into posts (title, content, author_id) values ('T1', 'C1', 1);
      insert into posts (title, content, author_id) values ('T2', 'C2', 1);
      insert into posts (title, content, author_id) values ('T3', 'C3', 2);
      insert into posts (title, content, author_id) values ('T4', 'C4', 2);
    SQL

    @sql = <<~SQL
      select
        posts.id, posts.title, posts.content,
        authors.id, authors.name
      from posts
      left outer join authors
      on posts.author_id = authors.id
      order by posts.id
    SQL

    @transform = {
      identity_idx: 0,
      columns: [
        :id,
        :title,
        :content,
        {
          name: :author,
          identity_idx: 3,
          columns: [:id, :name]
        }
      ]
    }
  end

  def test_transform_one_to_one_to_h
    t = Extralite::Transform.new(@transform)
    assert_equal @transform, t.to_h
  end

  def test_transform_one_to_one_query
    t = Extralite::Transform.new(@transform)
    result = @db.query(t, @sql)
    assert_kind_of Array, result
    assert_equal 4, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal({ id: 1, name: 'Foo' }, result[0][:author])

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal({ id: 1, name: 'Foo' }, result[1][:author])
    assert_equal result[0][:author].object_id, result[1][:author].object_id

    assert_equal 'T3', result[2][:title]
    assert_equal 'C3', result[2][:content]
    assert_equal({ id: 2, name: 'Bar' }, result[2][:author])

    assert_equal 'T4', result[3][:title]
    assert_equal 'C4', result[3][:content]
    assert_equal({ id: 2, name: 'Bar' }, result[3][:author])
    assert_equal result[2][:author].object_id, result[3][:author].object_id
  end

  def test_transform_one_to_one_query_with_block
    t = Extralite::Transform.new(@transform)
    result = []
    ret = @db.query(t, @sql) { result << it }
    assert_equal @db, ret
    assert_equal 4, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal({ id: 1, name: 'Foo' }, result[0][:author])

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal({ id: 1, name: 'Foo' }, result[1][:author])
    assert_equal result[0][:author].object_id, result[1][:author].object_id

    assert_equal 'T3', result[2][:title]
    assert_equal 'C3', result[2][:content]
    assert_equal({ id: 2, name: 'Bar' }, result[2][:author])

    assert_equal 'T4', result[3][:title]
    assert_equal 'C4', result[3][:content]
    assert_equal({ id: 2, name: 'Bar' }, result[3][:author])
    assert_equal result[2][:author].object_id, result[3][:author].object_id
  end
end

class TransformOneToManyTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.pragma('foreign_keys' => 1)
    @db.execute <<~SQL
      create table posts (
        id integer primary key,
        title text,
        content text
      );
      create table comments (
        id integer primary key,
        post_id integer references posts(id) on delete cascade,
        content text
      );

      insert into posts (title, content) values ('T1', 'C1');
      insert into posts (title, content) values ('T2', 'C2');
      insert into comments (post_id, content) values (1, 'comment 1');
      insert into comments (post_id, content) values (1, 'comment 2');
      insert into comments (post_id, content) values (2, 'comment 3');
      insert into comments (post_id, content) values (2, 'comment 4');
      insert into comments (post_id, content) values (2, 'comment 5');
    SQL

    @sql = <<~SQL
      select
        posts.id, posts.title, posts.content,
        comments.id, comments.content
      from posts
      left outer join comments
      on comments.post_id = posts.id
      order by posts.id, comments.id
    SQL

    @transform = {
      identity_idx: 0,
      columns: [
        :id,
        :title,
        :content,
        [{
          name: :comments,
          identity_idx: 3,
          columns: [:id, :content]
        }]
      ]
    }
  end

  def test_transform_one_to_many_to_h
    t = Extralite::Transform.new(@transform)
    assert_equal @transform, t.to_h
  end

  def test_transform_one_to_many_query
    t = Extralite::Transform.new(@transform)
    result = @db.query(t, @sql)
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, content: 'comment 1' },
      { id: 2, content: 'comment 2' }
    ], result[0][:comments]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 3, content: 'comment 3' },
      { id: 4, content: 'comment 4' },
      { id: 5, content: 'comment 5' }
    ], result[1][:comments]
  end

  def test_transform_one_to_many_query_with_block
    t = Extralite::Transform.new(@transform)
    result = []
    ret = @db.query(t, @sql) { result << it }
    assert_equal @db,  ret
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, content: 'comment 1' },
      { id: 2, content: 'comment 2' }
    ], result[0][:comments]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 3, content: 'comment 3' },
      { id: 4, content: 'comment 4' },
      { id: 5, content: 'comment 5' }
    ], result[1][:comments]
  end
end
