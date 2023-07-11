require_relative './extralite_ext'
require_relative './extralite/sqlite3_constants'

# Extralite is a Ruby gem for working with SQLite databases
module Extralite

  # The following error classes are already defined in the C extension. We put
  # them here for the sake of generating docs.

  # A base class for Extralite exceptions
  class Error < ::StandardError
  end

  # An exception representing an SQL error emitted by SQLite
  class SQLError < Error
  end

  # An exception raised when an SQLite database is busy (locked by another
  # thread or process)
  class BusyError < Error
  end

  # An exception raised when a query is interrupted by calling
  # `Database#interrupt` from another thread
  class InterruptError < Error
  end

  # An SQLite database
  class Database
    # @!visibility private
    TABLES_SQL = <<~SQL
      SELECT name FROM sqlite_master
      WHERE type ='table'
        AND name NOT LIKE 'sqlite_%';
    SQL

    # Returns the list of currently defined tables.
    #
    # @return [Array] list of tables
    def tables
      query_single_column(TABLES_SQL)
    end

    # Gets or sets one or more pragmas:
    #
    #     db.pragma(:cache_size) # get
    #     db.pragma(cache_size: -2000) # set
    #
    # @param value [Symbol, String, Hash] pragma name or hash mapping names to values
    # @return [Hash] query result
    def pragma(value)
      value.is_a?(Hash) ? pragma_set(value) : pragma_get(value)
    end

    private

    def pragma_set(values)
      sql = values.inject(+'') { |s, (k, v)| s += "pragma #{k}=#{v}; " }
      query(sql)
    end

    def pragma_get(key)
      query("pragma #{key}")
    end
  end
end
