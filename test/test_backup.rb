# frozen_string_literal: true

require_relative 'helper'

class BackupTest < MiniTest::Test
  SQLITE_OK   = 0
  SQLITE_DONE = 101

  def setup
    @src = Extralite::Database.new(':memory:')
    @dst = Extralite::Database.new(':memory:')

    @dst.query('create table t (x,y,z)')
    @src.query('create table t (x,y,z)')

    @dst.query('insert into t values (1, 2, 3)')
    @dst.query('insert into t values (4, 5, 6)')
  end

  def test_do_mem_backup_with_remaining
    b = Extralite::Backup.new(@dst, "main", @src, "main")
    begin
      b.step(1) # p [b.remaining, b.pagecount]
    end while b.remaining > 0
    assert_operator 0, :<, b.pagecount
    b.finish
  end

  def test_do_mem_backup_with_code
    b = Extralite::Backup.new(@dst, "main", @src, "main")
    ok = SQLITE_OK; ok = b.step(1) while ok == SQLITE_OK
    assert_operator 0, :<, b.pagecount
    assert_equal 0, b.remaining
    b.finish
    raise "omg: #{ok}" if ok != SQLITE_DONE
  end

  def test_closed
    assert_raises(Extralite::Error) do
      Extralite::Backup.new(Extralite::Database.new(':memory:').close, "main", @src, "main")
    end
    assert_raises(Extralite::Error) do
      Extralite::Backup.new(@dst, "main", Extralite::Database.new(':memory:').close, "main")
    end
  end
end
