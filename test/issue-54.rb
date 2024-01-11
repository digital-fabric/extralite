# frozen_string_literal: true

require "./lib/extralite"

puts 'Connecting to database...'

connection_1 = Extralite::Database.new("test.sqlite3")
puts "#{connection_1} connected"
connection_2 = Extralite::Database.new("test.sqlite3")
connection_2.busy_timeout = 0
puts "#{connection_2} connected"

[connection_1, connection_2].each do |connection|
  puts "#{connection} beginning transaction..."
  connection.execute "begin immediate transaction"
end

[connection_1, connection_2].each do |connection|
  puts "#{connection} rolling back transaction..."
  connection.execute "rollback transaction"
end
