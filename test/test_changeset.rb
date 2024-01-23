# frozen_string_literal: true

require_relative 'helper'

require 'date'
require 'tempfile'

class ChangesetTest < MiniTest::Test
  def setup
    @db = Extralite::Database.new(':memory:')
    skip if !@db.respond_to?(:track_changes)

    @db.execute('create table if not exists t (x integer primary key, y, z)')
  end

  def test_each
    changeset = Extralite::Changeset.new

    changeset.track(@db, [nil]) do
      @db.execute('insert into t values (1, 2, 3)')
    end
    changes = []
    changeset.each { |*o| changes << o }
    assert_equal [
      [:insert, 't', nil, [1, 2, 3]]
    ], changes


    changeset.track(@db, [nil]) do
      @db.execute('update t set y = 22 where x = 1')
    end
    changes = []
    changeset.each { |*o| changes << o }
    assert_equal [
      [:update, 't', [1, 2, nil], [nil, 22, nil]]
    ], changes


    changeset.track(@db, [nil]) do
      @db.execute('delete from t where x = 1')
    end
    changes = []
    changeset.each { |*o| changes << o }
    assert_equal [
      [:delete, 't', [1, 22, 3], nil]
    ], changes
  end

  def test_to_a
    changeset = Extralite::Changeset.new

    changeset.track(@db, [nil]) do
      @db.execute('insert into t values (1, 2, 3)')
      @db.execute('insert into t values (4, 5, 6)')
    end
    assert_equal [
      [:insert, 't', nil, [1, 2, 3]],
      [:insert, 't', nil, [4, 5, 6]]
    ], changeset.to_a


    changeset.track(@db, [nil]) do
      @db.execute('update t set y = 22.22 where z < 10')
    end
    assert_equal [
      [:update, 't', [1, 2, nil], [nil, 22.22, nil]],
      [:update, 't', [4, 5, nil], [nil, 22.22, nil]]
    ], changeset.to_a


    changeset.track(@db, [nil]) do
      @db.execute('delete from t where x = 1')
    end
    assert_equal [
      [:delete, 't', [1, 22.22, 3], nil]
    ], changeset.to_a
  end

  def test_apply
    changeset = Extralite::Changeset.new

    changeset.track(@db, [:t]) do
      @db.execute('insert into t values (1, 2, 3)')
      @db.execute('insert into t values (4, 5, 6)')
    end

    db2 = Extralite::Database.new(':memory:')
    db2.execute('create table if not exists t (x integer primary key, y, z)')

    changeset.apply(db2)

    assert_equal [
      { x: 1, y: 2, z: 3 },
      { x: 4, y: 5, z: 6 }
    ], db2.query('select * from t')
  end

  def test_invert
    changeset = Extralite::Changeset.new

    changeset.track(@db, [:t]) do
      @db.execute('insert into t values (1, 2, 3)')
      @db.execute('insert into t values (4, 5, 6)')
    end

    db2 = Extralite::Database.new(':memory:')
    db2.execute('create table if not exists t (x integer primary key, y, z)')

    changeset.apply(db2)

    assert_equal [
      { x: 1, y: 2, z: 3 },
      { x: 4, y: 5, z: 6 }
    ], db2.query('select * from t')

    db2.execute('insert into t values (7, 8, 9)')
    inverted = changeset.invert

    assert_kind_of Extralite::Changeset, inverted
    refute_equal inverted, changeset

    assert_equal [
      [:delete, 't', [1, 2, 3], nil],
      [:delete, 't', [4, 5, 6], nil],
    ], inverted.to_a

    inverted.apply(@db)
    assert_equal [], @db.query('select * from t')
  end

  def test_blob
    changeset = Extralite::Changeset.new
    assert_equal "", changeset.to_blob

    changeset.track(@db, [:t]) do
      @db.execute('insert into t values (1, 2, 3)')
      @db.execute('insert into t values (4, 5, 6)')
    end

    blob = changeset.to_blob
    assert_kind_of String, blob
    assert_equal Encoding::ASCII_8BIT, blob.encoding
    assert !blob.empty?

    c2 = Extralite::Changeset.new
    c2.load(blob)
    assert_equal c2.to_blob, blob

    assert_equal [
      [:insert, 't', nil, [1, 2, 3]],
      [:insert, 't', nil, [4, 5, 6]]
    ], c2.to_a
  end

  def test_empty_blob
    changeset = Extralite::Changeset.new
    changeset.load('')

    assert_raises(Extralite::Error) { changeset.to_a }
  end
end
