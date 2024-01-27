# frozen_string_literal: true

require './lib/extralite'

db = Extralite::Database.new(':memory:')
count = 0
db.on_progress(10) { count += 1 }
10000.times { db.query('select 1') }
p count: count
