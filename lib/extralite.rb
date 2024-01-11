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

  # An exception raised when an Extralite doesn't know how to bind a parameter to a query
  class ParameterError < Error
  end

  # An SQLite database
  class Database
    # @!visibility private
    TABLES_SQL = <<~SQL
      SELECT name FROM %<db>s.sqlite_master
      WHERE type ='table'
        AND name NOT LIKE 'sqlite_%%';
    SQL

    alias_method :execute_multi, :batch_execute

    # Returns the list of currently defined tables. If a database name is given,
    # returns the list of tables for the relevant attached database.
    #
    # @param db [String] name of attached database
    # @return [Array] list of tables
    def tables(db = 'main')
      query_single_column(format(TABLES_SQL, db: db))
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

    # Starts a transaction and runs the given block. If an exception is raised
    # in the block, the transaction is rolled back. Otherwise, the transaction
    # is commited after running the block.
    #
    #     db.transaction do
    #       db.execute('insert into foo values (1, 2, 3)')
    #       raise if db.query_single_value('select x from bar') > 42
    #     end
    #
    # @param mode [Symbol, String] transaction mode (deferred, immediate or exclusive). Defaults to immediate.
    # @return [Any] the given block's return value
    def transaction(mode = :immediate)
      execute "begin #{mode} transaction"
    
      abort = false
      yield self
    rescue
      abort = true
      raise
    ensure
      execute(abort ? 'rollback' : 'commit')
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

  class Query
    alias_method :execute_multi, :batch_execute
  end
end
