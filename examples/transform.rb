# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  gem 'extralite', path: '.'
end

db = Extralite::Database.new(':memory:')
db.pragma('foreign_keys' => 1)
db.execute <<~SQL
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

transform = Extralite::Transform.new do
  {
    id:       integer.identity,
    title:    text,
    content:  text,
    tags:     [{
      id:     integer.identity,
      name:   text
    }]
  }
end

require 'pp'
PP.pp db.query(transform, sql), $stdout, 40
