# frozen_string_literal: true

require_relative 'helper'

class ExtraliteTest < Minitest::Test
  def test_sqlite3_version
    assert_match(/^3\.\d+\.\d+$/, Extralite.sqlite3_version)
  end

  def test_status
    db = Extralite::Database.new(':memory:')
    db.query('create table if not exists t (x,y,z)')
    db.query('insert into t values (1, 2, 3)')

    begin
      a = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED, false)
      b = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED)
      c = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED, true)
      d = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED, true)

      assert_operator 0, :<, a[0]
      assert_operator a[0], :<=, a[1]

      assert_equal a, b
      assert_equal a, c

      assert_equal a[0], d[0]
      assert_equal a[0], d[1]
    ensure
      db.close
    end
  end
end
