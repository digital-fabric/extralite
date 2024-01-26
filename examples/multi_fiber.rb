# frozen_string_literal: true

require './lib/extralite'
require 'fiber'

f1 = Fiber.new do |f2|
  while true
    STDOUT << '.'
    f2 = f2.transfer
  end
end

db = Extralite::Database.new(':memory:')
db.on_progress(1) { f1.transfer(Fiber.current) }

p db.query('select 1, 2, 3')
