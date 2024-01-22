# frozen_string_literal: true

require_relative 'helper'

require 'date'
require 'tempfile'

class ChangesetTest < MiniTest::Test
  def setup
    fn = Tempfile.new('extralite_test_changeset').path
    @db = Extralite::Database.new(fn)
    skip if !@db.respond_to?(:track_changes)

    @db.query('create table if not exists t (x integer primary key, y, z)')
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
end
