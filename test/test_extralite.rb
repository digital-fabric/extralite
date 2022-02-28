# frozen_string_literal: true

require_relative 'helper'

class ExtraliteTest < MiniTest::Test
  def test_sqlite3_version
    assert_match /^3\.\d+\.\d+$/, Extralite.sqlite3_version
  end
end
