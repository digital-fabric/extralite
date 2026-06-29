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

    @spec = {
      columns: {
        id:       { identity: true },
        title:    {},
        content:  {},
        author:   {
          type:     :relation,
          columns:  {
            id:   { identity: true },
            name: {}
          }
        }
      }
    }
  end

  def test_transform_one_to_one_to_h
    t = Extralite::Transform.new(@spec)
    assert_equal @spec, t.to_h
  end

  def test_transform_one_to_one_query
    t = Extralite::Transform.new(@spec)
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
    t = Extralite::Transform.new(@spec)
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

    @spec = {
      columns: {
        id:       { identity: true},
        title:    {},
        content:  {},
        comments: [{
          type: :relation,
          columns: {
            id:       { identity: true },
            content:  {}
          }
        }]
      }
    }
  end

  def test_transform_one_to_many_to_h
    t = Extralite::Transform.new(@spec)
    assert_equal @spec, t.to_h
  end

  def test_transform_one_to_many_query
    t = Extralite::Transform.new(@spec)
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
    t = Extralite::Transform.new(@spec)
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

class TransformManyToManyTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.pragma('foreign_keys' => 1)
    @db.execute <<~SQL
      create table posts (
        id integer primary key,
        title text,
        content text
      );
      create table tags (
        id integer primary key,
        name text
      );
      create table posts_tags (
        post_id integer references posts(id) on delete cascade,
        tag_id integer references tags(id) on delete cascade
      );

      insert into posts (title, content) values ('T1', 'C1');
      insert into posts (title, content) values ('T2', 'C2');
      insert into tags (name) values ('tag1');
      insert into tags (name) values ('tag2');
      insert into tags (name) values ('tag3');

      insert into posts_tags(post_id, tag_id) values (1, 1);
      insert into posts_tags(post_id, tag_id) values (1, 2);
      insert into posts_tags(post_id, tag_id) values (2, 2);
      insert into posts_tags(post_id, tag_id) values (2, 3);
    SQL

    @sql = <<~SQL
      select
        posts.id, posts.title, posts.content,
        tags.id, tags.name
      from posts
      left outer join posts_tags on posts_tags.post_id = posts.id
      left outer join tags on posts_tags.tag_id = tags.id
      order by posts.id, tags.id
    SQL

    @spec = {
      columns: {
        id: { identity: true },
        title: {},
        content: {},
        tags: [{
          type: :relation,
          columns: {
            id: { identity: true },
            name: {}
          }
        }]
      }
    }
  end

  def test_transform_many_to_many_to_h
    t = Extralite::Transform.new(@spec)
    assert_equal @spec, t.to_h
  end

  def test_transform_many_to_many_query
    t = Extralite::Transform.new(@spec)
    result = @db.query(t, @sql)
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, name: 'tag1' },
      { id: 2, name: 'tag2' }
    ], result[0][:tags]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 2, name: 'tag2' },
      { id: 3, name: 'tag3' },
    ], result[1][:tags]
    assert_equal result[0][:tags][1].object_id, result[1][:tags][0].object_id
  end

  def test_transform_many_to_many_query_with_block
    t = Extralite::Transform.new(@spec)
    result = []
    ret = @db.query(t, @sql) { result << it }
    assert_equal @db,  ret
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, name: 'tag1' },
      { id: 2, name: 'tag2' }
    ], result[0][:tags]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 2, name: 'tag2' },
      { id: 3, name: 'tag3' },
    ], result[1][:tags]
    assert_equal result[0][:tags][1].object_id, result[1][:tags][0].object_id
  end

  def test_transform_many_to_many_query_single
    t = Extralite::Transform.new(@spec)
    result = @db.query_single(t, @sql)

    assert_equal 'T1', result[:title]
    assert_equal 'C1', result[:content]
    assert_equal [
      { id: 1, name: 'tag1' },
      { id: 2, name: 'tag2' }
    ], result[:tags]
  end
end

class TransformErrorTest < Minitest::Test
  def test_transform_bad_spec
    spec = {
      columnss: [
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
    assert_raises(Extralite::Error) { Extralite::Transform.new(spec) }
  end
end

class TransformTypesTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.pragma('foreign_keys' => 1)
  end

  def test_transform_types_to_h
    spec = {
      columns: {
        id:       { type: :integer, identity: true },
        title:    { type: :text },
        content:  { type: :text },
        tags:     [{
          type: :relation,
          columns: {
            id:   { type: :integer, identity: true },
            name: { type: :text }
          }
        }]
      }
      #   { name: :x, type: :integer },
      #   { name: :y, type: :integer }
      # ]
    }


    # spec = {
    #   identity_idx: 0,
    #   columns: {

    #   }
    #     { name: :x, type: :integer },
    #     { name: :y, type: :integer }
    #   ]
    # }
    t = Extralite::Transform.new(spec)
    assert_equal spec, t.to_h
  end

  def test_transform_types_coercion
    t = Extralite::Transform.new(
      columns: {
        x: { type: :integer },
        y: { type: :integer }
      }
    )
    result = @db.query(t, <<~SQL)
      select 'foo', '42'
    SQL
    assert_equal [
      { x: 0, y: 42 }
    ], result

    t = Extralite::Transform.new(
      columns: {
        x: { type: :text },
        y: { type: :integer }
      }
    )
    result = @db.query(t, <<~SQL)
      select 42, '42'
    SQL
    assert_equal [
      { x: '42', y: 42 }
    ], result

    t = Extralite::Transform.new(
      columns: {
        x: { type: :text },
        y: { type: :float }
      }
    )
    result = @db.query(t, <<~SQL)
      select 42, 42
    SQL
    assert_equal [
      { x: '42', y: 42.0 }
    ], result

    t = Extralite::Transform.new(
      columns: {
        x: { type: :bool },
        y: { type: :bool },
        z: { type: -> (x) { x.to_i * 2 } }
      }
    )
    result = @db.query(t, <<~SQL)
      select 42, 'foo', 42
    SQL
    assert_equal [
      { x: true, y: false, z: 84 }
    ], result

    # NULL handling
    t = Extralite::Transform.new(
      columns: {
        a: { type: nil },
        b: { type: :integer },
        c: { type: :float },
        d: { type: :text },
        e: { type: :bool },
        f: { type: :json },
        g: { type: ->(x) { x ? x.to_i * 2 : :null } }
      }
    )
    result = @db.query(t, <<~SQL)
      select null, null, null, null, null, null, null
    SQL
    assert_equal [
      { a: nil, b: nil, c: nil, d: nil, e: nil, f: nil, g: :null }
    ], result

    # JSON parsing
    require 'json'
    t = Extralite::Transform.new(
      columns: {
        a: {},
        b: { type: :json }
      }
    )
    result = @db.query(t, <<~SQL)
      select 42, '[1, 2, 3]'
    SQL
    assert_equal [
      { a: 42, b: [1, 2, 3] }
    ], result

    result = @db.query(t, <<~SQL)
      select 42, '{ "a": [1, 2, 3], "blah": "hi"}'
    SQL
    assert_equal [
      { a: 42, b: { 'a' => [1, 2, 3], 'blah' => 'hi' } }
    ], result
  end

  def test_transform_types_with_relations
    @db.execute <<~SQL
      create table posts (
        id integer primary key,
        title text,
        content text
      );
      create table tags (
        id integer primary key,
        name text
      );
      create table posts_tags (
        post_id integer references posts(id) on delete cascade,
        tag_id integer references tags(id) on delete cascade
      );

      insert into posts (title, content) values ('T1', 'C1');
      insert into posts (title, content) values ('T2', 'C2');
      insert into tags (name) values ('tag1');
      insert into tags (name) values ('tag2');
      insert into tags (name) values ('tag3');

      insert into posts_tags(post_id, tag_id) values (1, 1);
      insert into posts_tags(post_id, tag_id) values (1, 2);
      insert into posts_tags(post_id, tag_id) values (2, 2);
      insert into posts_tags(post_id, tag_id) values (2, 3);
    SQL

    sql = <<~SQL
      select
        posts.id, posts.title, posts.content,
        tags.id, tags.name
      from posts
      left outer join posts_tags on posts_tags.post_id = posts.id
      left outer join tags on posts_tags.tag_id = tags.id
      order by posts.id, tags.id
    SQL

    spec = {
      columns: {
        id: { type: :integer, identity: true },
        title: { type: :text },
        content: { type: :text },
        tags: [{
          type: :relation,
          columns: {
            id: { type: :integer, identity: true },
            name: { type: :text }
          }
        }]
      }
    }

    t = Extralite::Transform.new(spec)
    result = @db.query(t, sql)
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, name: 'tag1' },
      { id: 2, name: 'tag2' }
    ], result[0][:tags]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 2, name: 'tag2' },
      { id: 3, name: 'tag3' },
    ], result[1][:tags]
    assert_equal result[0][:tags][1].object_id, result[1][:tags][0].object_id
  end
end

class TransformPreparedQueryTest < Minitest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    @db.pragma('foreign_keys' => 1)
    @db.execute <<~SQL
      create table posts (
        id integer primary key,
        title text,
        content text
      );
      create table tags (
        id integer primary key,
        name text
      );
      create table posts_tags (
        post_id integer references posts(id) on delete cascade,
        tag_id integer references tags(id) on delete cascade
      );

      insert into posts (title, content) values ('T1', 'C1');
      insert into posts (title, content) values ('T2', 'C2');
      insert into tags (name) values ('tag1');
      insert into tags (name) values ('tag2');
      insert into tags (name) values ('tag3');

      insert into posts_tags(post_id, tag_id) values (1, 1);
      insert into posts_tags(post_id, tag_id) values (1, 2);
      insert into posts_tags(post_id, tag_id) values (2, 2);
      insert into posts_tags(post_id, tag_id) values (2, 3);
    SQL

    @sql = <<~SQL
      select
        posts.id, posts.title, posts.content,
        tags.id, tags.name
      from posts
      left outer join posts_tags on posts_tags.post_id = posts.id
      left outer join tags on posts_tags.tag_id = tags.id
      order by posts.id, tags.id
    SQL

    @spec = {
      columns: {
        id: { identity: true },
        title: {},
        content: {},
        tags: [{
          type: :relation,
          columns: {
            id: { identity: true },
            name: {}
          }
        }]
      }
    }
  end

  def test_transform_prepared_query
    t = Extralite::Transform.new(@spec)
    q = @db.prepare(t, @sql)

    result = q.to_a
    assert_kind_of Array, result
    assert_equal 2, result.size

    assert_equal 'T1', result[0][:title]
    assert_equal 'C1', result[0][:content]
    assert_equal [
      { id: 1, name: 'tag1' },
      { id: 2, name: 'tag2' }
    ], result[0][:tags]

    assert_equal 'T2', result[1][:title]
    assert_equal 'C2', result[1][:content]
    assert_equal [
      { id: 2, name: 'tag2' },
      { id: 3, name: 'tag3' },
    ], result[1][:tags]
    assert_equal result[0][:tags][1].object_id, result[1][:tags][0].object_id
  end
end
