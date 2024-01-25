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

  # This class encapsulates an SQLite database connection.
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
      query_argv(format(TABLES_SQL, db: db))
    end

    # Gets or sets one or more database pragmas. For a list of available pragmas
    # see: https://sqlite.org/pragma.html#toc
    #
    #     db.pragma(:cache_size) # get
    #     db.pragma(cache_size: -2000) # set
    #
    # @param value [Symbol, String, Hash] pragma name or hash mapping names to values
    # @return [Hash] query result
    def pragma(value)
      value.is_a?(Hash) ? pragma_set(value) : pragma_get(value)
    end

    # Error class used to roll back a transaction without propagating an
    # exception.
    class Rollback < Error
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
    # For more information on transactions see:
    # https://sqlite.org/lang_transaction.html
    #
    # @param mode [Symbol, String] transaction mode (deferred, immediate or exclusive).
    # @return [Any] the given block's return value
    def transaction(mode = :immediate)
      execute "begin #{mode} transaction"
    
      abort = false
      yield self
    rescue => e
      abort = true
      raise unless e.is_a?(Rollback)
    ensure
      execute(abort ? 'rollback' : 'commit')
    end

    # Creates a savepoint with the given name. For more information on
    # savepoints see: https://sqlite.org/lang_savepoint.html
    #
    #     db.savepoint(:savepoint1)
    #     db.execute('insert into foo values (42)')
    #     db.rollback_to(:savepoint1)
    #     db.release(:savepoint1)
    #
    # @param name [String, Symbol] savepoint name
    # @return [Extralite::Database] database
    def savepoint(name)
      execute "savepoint #{name}"
      self
    end

    # Release a savepoint with the given name. For more information on
    # savepoints see: https://sqlite.org/lang_savepoint.html
    #
    # @param name [String, Symbol] savepoint name
    # @return [Extralite::Database] database
    def release(name)
      execute "release #{name}"
      self
    end

    # Rolls back changes to a savepoint with the given name. For more
    # information on savepoints see: https://sqlite.org/lang_savepoint.html
    #
    # @param name [String, Symbol] savepoint name
    # @return [Extralite::Database] database
    def rollback_to(name)
      execute "rollback to #{name}"
      self
    end

    # Rolls back the currently active transaction. This method should only be
    # called from within a block passed to `Database#transaction`. This method
    # raises a Extralite::Rollback exception, which will stop execution of the
    # transaction block without propagating the exception.
    #
    #     db.transaction do
    #       db.execute('insert into foo (42)')
    #       db.rollback!
    #     end
    #
    # @param name [String, Symbol] savepoint name
    # @return [Extralite::Database] database
    def rollback!
      raise Rollback
    end

    private

    def pragma_set(values)
      sql = values.inject(+'') { |s, (k, v)| s += "pragma #{k}=#{v}; " }
      query(sql)
    end

    def pragma_get(key)
      query_single_argv("pragma #{key}")
    end
  end

  class Query
    alias_method :execute_multi, :batch_execute
  end
end
