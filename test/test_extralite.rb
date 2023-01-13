# frozen_string_literal: true

require_relative 'helper'

class ExtraliteTest < MiniTest::Test
  def test_sqlite3_version
    assert_match /^3\.\d+\.\d+$/, Extralite.sqlite3_version
  end

  def test_status
    db = Extralite::Database.new(':memory:')
    db.query('create table if not exists t (x,y,z)')
    db.query('insert into t values (1, 2, 3)')

    begin
      a = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED, false) # [val, hwm]
      b = Extralite::runtime_status(Extralite::SQLITE_STATUS_MEMORY_USED) # [val, hwm]
      c = Extralite::runtime_status # val for Extralite::SQLITE_STATUS_MEMORY_USED
      assert_operator 0, :<, a[0]
      assert_operator 0, :<, b[0]
      assert_operator 0, :<, c[0]
    ensure
      db.close
    end
  end
end
