# frozen_string_literal: true

require_relative 'helper'

class ExtraliteTest < MiniTest::Test
  def test_sqlite3_version
    reported = `sqlite3 --version` rescue ''
    match = reported.match(/^([\d\.]+)/)
    if match
      assert_equal match[1], Extralite.sqlite3_version
    end
  end
end
