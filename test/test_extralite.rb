# frozen_string_literal: true

require_relative 'helper'

class ExtraliteTest < MiniTest::Test
  SQLITE3_C_PATH = File.expand_path('../ext/extralite/sqlite3.c', __dir__)
  SQLITE_VERSION_DEFINE_REGEXP = /#define SQLITE_VERSION\s+"([\d\.]+)"/m.freeze

  def test_sqlite3_version
    version = IO.read(SQLITE3_C_PATH).match(SQLITE_VERSION_DEFINE_REGEXP)[1]

    assert_equal version, Extralite.sqlite3_version
  end
end
