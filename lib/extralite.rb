require_relative './extralite_ext'

# Extralite is a Ruby gem for working with SQLite databases
module Extralite
  # A base class for Extralite exceptions
  class Error < RuntimeError
  end

  # An exception representing an SQL error emitted by SQLite
  class SQLError < Error
  end

  # An exception raised when an SQLite database is busy (locked by another
  # thread or process)
  class BusyError < Error
  end

  # An SQLite database
  class Database
  end
end