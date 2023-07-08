# frozen_string_literal: true

require_relative 'helper'
require 'sequel'

class SequelExtraliteTest < MiniTest::Test
  def setup
    @db = Sequel.connect('extralite::memory:')
    @db.create_table :items do
      primary_key :id
      String :name, unique: true, null: false
      Float :price, null: false
    end

    items = @db[:items]
    items.insert(name: 'abc', price: 123)
    items.insert(name: 'def', price: 456)
    items.insert(name: 'ghi', price: 789)
  end

  def teardown
    @db.disconnect
  end

  def test_sequel
    items = @db[:items]

    assert_equal 3, items.count
    assert_equal (123+456+789) / 3, items.avg(:price)
  end

  def test_prepared_statement
    items = @db[:items]
    prepared_query = items.where(name: :$name).prepare(:select, :select_by_name)
    prepared_insert = items.prepare(:insert, :insert_with_name_and_price, name: :$name, price: :$price)

    assert_equal [{ id: 2, name: 'def', price: 456 }], prepared_query.call(name: 'def')
    assert_equal [{ id: 2, name: 'def', price: 456 }], @db.call(:select_by_name, name: 'def')

    id = prepared_insert.call(name: 'jkl', price: 444)
    assert_equal({ id: id, name: 'jkl', price: 444 }, items[id: id])

    id = @db.call(:insert_with_name_and_price, name: 'mno', price: 555)
    assert_equal({ id: id, name: 'mno', price: 555 }, items[id: id])
  end

  def test_migration
    # Adapted from https://github.com/digital-fabric/extralite/issues/8
    Dir.mktmpdir("extralite-migration") do |dir|
      File.write(dir + "/001_migrate.rb", <<~RUBY)
        Sequel.migration do 
          change do
            create_table(:foobars) { primary_key :id } 
          end
        end
      RUBY
    
      Sequel.extension :migration
      db = Sequel.connect("extralite://")
      Sequel::Migrator.run(db, dir)

      assert_equal [:id], db[:foobars].columns
    end
  end
end
