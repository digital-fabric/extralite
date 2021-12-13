# frozen_string_literal: true

require_relative 'helper'
require 'sequel'

class SequelExtraliteTest < MiniTest::Test
  def test_sequel
    db = Sequel.connect('extralite::memory:')
    db.create_table :items do
      primary_key :id
      String :name, unique: true, null: false
      Float :price, null: false
    end

    items = db[:items]

    items.insert(name: 'abc', price: 123)
    items.insert(name: 'def', price: 456)
    items.insert(name: 'ghi', price: 789)

    assert_equal 3, items.count
    assert_equal (123+456+789) / 3, items.avg(:price)
  end
end
