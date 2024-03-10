require_relative './extralite_ext'

# Extralite is a Ruby gem for working with SQLite databases
module Extralite
  SQLITE_STATUS_MEMORY_USED           =  0
  SQLITE_STATUS_PAGECACHE_USED        =  1
  SQLITE_STATUS_PAGECACHE_OVERFLOW    =  2
  SQLITE_STATUS_SCRATCH_USED          =  3
  SQLITE_STATUS_SCRATCH_OVERFLOW      =  4
  SQLITE_STATUS_MALLOC_SIZE           =  5
  SQLITE_STATUS_PARSER_STACK          =  6
  SQLITE_STATUS_PAGECACHE_SIZE        =  7
  SQLITE_STATUS_SCRATCH_SIZE          =  8
  SQLITE_STATUS_MALLOC_COUNT          =  9

  SQLITE_DBSTATUS_LOOKASIDE_USED      =  0
  SQLITE_DBSTATUS_CACHE_USED          =  1
  SQLITE_DBSTATUS_SCHEMA_USED         =  2
  SQLITE_DBSTATUS_STMT_USED           =  3
  SQLITE_DBSTATUS_LOOKASIDE_HIT       =  4
  SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE =  5
  SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL =  6
  SQLITE_DBSTATUS_CACHE_HIT           =  7
  SQLITE_DBSTATUS_CACHE_MISS          =  8
  SQLITE_DBSTATUS_CACHE_WRITE         =  9
  SQLITE_DBSTATUS_DEFERRED_FKS        = 10
  SQLITE_DBSTATUS_CACHE_USED_SHARED   = 11
  SQLITE_DBSTATUS_CACHE_SPILL         = 12

  SQLITE_STMTSTATUS_FULLSCAN_STEP     =  1
  SQLITE_STMTSTATUS_SORT              =  2
  SQLITE_STMTSTATUS_AUTOINDEX         =  3
  SQLITE_STMTSTATUS_VM_STEP           =  4
  SQLITE_STMTSTATUS_REPREPARE         =  5
  SQLITE_STMTSTATUS_RUN               =  6
  SQLITE_STMTSTATUS_FILTER_MISS       =  7
  SQLITE_STMTSTATUS_FILTER_HIT        =  8
  SQLITE_STMTSTATUS_MEMUSED           = 99

  SQLITE_LIMIT_LENGTH                 =  0
  SQLITE_LIMIT_SQL_LENGTH             =  1
  SQLITE_LIMIT_COLUMN                 =  2
  SQLITE_LIMIT_EXPR_DEPTH             =  3
  SQLITE_LIMIT_COMPOUND_SELECT        =  4
  SQLITE_LIMIT_VDBE_OP                =  5
  SQLITE_LIMIT_FUNCTION_ARG           =  6
  SQLITE_LIMIT_ATTACHED               =  7
  SQLITE_LIMIT_LIKE_PATTERN_LENGTH    =  8
  SQLITE_LIMIT_VARIABLE_NUMBER        =  9
  SQLITE_LIMIT_TRIGGER_DEPTH          = 10
  SQLITE_LIMIT_WORKER_THREADS         = 11

  SQLITE_OK                           =   0
  SQLITE_ERROR                        =   1
  SQLITE_INTERNAL                     =   2
  SQLITE_PERM                         =   3
  SQLITE_ABORT                        =   4
  SQLITE_BUSY                         =   5
  SQLITE_LOCKED                       =   6
  SQLITE_NOMEM                        =   7
  SQLITE_READONLY                     =   8
  SQLITE_INTERRUPT                    =   9
  SQLITE_IOERR                        =  10
  SQLITE_CORRUPT                      =  11
  SQLITE_NOTFOUND                     =  12
  SQLITE_FULL                         =  13
  SQLITE_CANTOPEN                     =  14
  SQLITE_PROTOCOL                     =  15
  SQLITE_EMPTY                        =  16
  SQLITE_SCHEMA                       =  17
  SQLITE_TOOBIG                       =  18
  SQLITE_CONSTRAINT                   =  19
  SQLITE_MISMATCH                     =  20
  SQLITE_MISUSE                       =  21
  SQLITE_NOLFS                        =  22
  SQLITE_AUTH                         =  23
  SQLITE_FORMAT                       =  24
  SQLITE_RANGE                        =  25
  SQLITE_NOTADB                       =  26
  SQLITE_NOTICE                       =  27
  SQLITE_WARNING                      =  28
  SQLITE_ROW                          = 100
  SQLITE_DONE                         = 101

  SQLITE_ERROR_MISSING_COLLSEQ        = (SQLITE_ERROR | (1<<8))
  SQLITE_ERROR_RETRY                  = (SQLITE_ERROR | (2<<8))
  SQLITE_ERROR_SNAPSHOT               = (SQLITE_ERROR | (3<<8))
  SQLITE_IOERR_READ                   = (SQLITE_IOERR | (1<<8))
  SQLITE_IOERR_SHORT_READ             = (SQLITE_IOERR | (2<<8))
  SQLITE_IOERR_WRITE                  = (SQLITE_IOERR | (3<<8))
  SQLITE_IOERR_FSYNC                  = (SQLITE_IOERR | (4<<8))
  SQLITE_IOERR_DIR_FSYNC              = (SQLITE_IOERR | (5<<8))
  SQLITE_IOERR_TRUNCATE               = (SQLITE_IOERR | (6<<8))
  SQLITE_IOERR_FSTAT                  = (SQLITE_IOERR | (7<<8))
  SQLITE_IOERR_UNLOCK                 = (SQLITE_IOERR | (8<<8))
  SQLITE_IOERR_RDLOCK                 = (SQLITE_IOERR | (9<<8))
  SQLITE_IOERR_DELETE                 = (SQLITE_IOERR | (10<<8))
  SQLITE_IOERR_BLOCKED                = (SQLITE_IOERR | (11<<8))
  SQLITE_IOERR_NOMEM                  = (SQLITE_IOERR | (12<<8))
  SQLITE_IOERR_ACCESS                 = (SQLITE_IOERR | (13<<8))
  SQLITE_IOERR_CHECKRESERVEDLOCK      = (SQLITE_IOERR | (14<<8))
  SQLITE_IOERR_LOCK                   = (SQLITE_IOERR | (15<<8))
  SQLITE_IOERR_CLOSE                  = (SQLITE_IOERR | (16<<8))
  SQLITE_IOERR_DIR_CLOSE              = (SQLITE_IOERR | (17<<8))
  SQLITE_IOERR_SHMOPEN                = (SQLITE_IOERR | (18<<8))
  SQLITE_IOERR_SHMSIZE                = (SQLITE_IOERR | (19<<8))
  SQLITE_IOERR_SHMLOCK                = (SQLITE_IOERR | (20<<8))
  SQLITE_IOERR_SHMMAP                 = (SQLITE_IOERR | (21<<8))
  SQLITE_IOERR_SEEK                   = (SQLITE_IOERR | (22<<8))
  SQLITE_IOERR_DELETE_NOENT           = (SQLITE_IOERR | (23<<8))
  SQLITE_IOERR_MMAP                   = (SQLITE_IOERR | (24<<8))
  SQLITE_IOERR_GETTEMPPATH            = (SQLITE_IOERR | (25<<8))
  SQLITE_IOERR_CONVPATH               = (SQLITE_IOERR | (26<<8))
  SQLITE_IOERR_VNODE                  = (SQLITE_IOERR | (27<<8))
  SQLITE_IOERR_AUTH                   = (SQLITE_IOERR | (28<<8))
  SQLITE_IOERR_BEGIN_ATOMIC           = (SQLITE_IOERR | (29<<8))
  SQLITE_IOERR_COMMIT_ATOMIC          = (SQLITE_IOERR | (30<<8))
  SQLITE_IOERR_ROLLBACK_ATOMIC        = (SQLITE_IOERR | (31<<8))
  SQLITE_IOERR_DATA                   = (SQLITE_IOERR | (32<<8))
  SQLITE_IOERR_CORRUPTFS              = (SQLITE_IOERR | (33<<8))
  SQLITE_LOCKED_SHAREDCACHE           = (SQLITE_LOCKED |  (1<<8))
  SQLITE_LOCKED_VTAB                  = (SQLITE_LOCKED |  (2<<8))
  SQLITE_BUSY_RECOVERY                = (SQLITE_BUSY   |  (1<<8))
  SQLITE_BUSY_SNAPSHOT                = (SQLITE_BUSY   |  (2<<8))
  SQLITE_BUSY_TIMEOUT                 = (SQLITE_BUSY   |  (3<<8))
  SQLITE_CANTOPEN_NOTEMPDIR           = (SQLITE_CANTOPEN | (1<<8))
  SQLITE_CANTOPEN_ISDIR               = (SQLITE_CANTOPEN | (2<<8))
  SQLITE_CANTOPEN_FULLPATH            = (SQLITE_CANTOPEN | (3<<8))
  SQLITE_CANTOPEN_CONVPATH            = (SQLITE_CANTOPEN | (4<<8))
  SQLITE_CANTOPEN_DIRTYWAL            = (SQLITE_CANTOPEN | (5<<8))
  SQLITE_CANTOPEN_SYMLINK             = (SQLITE_CANTOPEN | (6<<8))
  SQLITE_CORRUPT_VTAB                 = (SQLITE_CORRUPT | (1<<8))
  SQLITE_CORRUPT_SEQUENCE             = (SQLITE_CORRUPT | (2<<8))
  SQLITE_CORRUPT_INDEX                = (SQLITE_CORRUPT | (3<<8))
  SQLITE_READONLY_RECOVERY            = (SQLITE_READONLY | (1<<8))
  SQLITE_READONLY_CANTLOCK            = (SQLITE_READONLY | (2<<8))
  SQLITE_READONLY_ROLLBACK            = (SQLITE_READONLY | (3<<8))
  SQLITE_READONLY_DBMOVED             = (SQLITE_READONLY | (4<<8))
  SQLITE_READONLY_CANTINIT            = (SQLITE_READONLY | (5<<8))
  SQLITE_READONLY_DIRECTORY           = (SQLITE_READONLY | (6<<8))
  SQLITE_ABORT_ROLLBACK               = (SQLITE_ABORT | (2<<8))
  SQLITE_CONSTRAINT_CHECK             = (SQLITE_CONSTRAINT | (1<<8))
  SQLITE_CONSTRAINT_COMMITHOOK        = (SQLITE_CONSTRAINT | (2<<8))
  SQLITE_CONSTRAINT_FOREIGNKEY        = (SQLITE_CONSTRAINT | (3<<8))
  SQLITE_CONSTRAINT_FUNCTION          = (SQLITE_CONSTRAINT | (4<<8))
  SQLITE_CONSTRAINT_NOTNULL           = (SQLITE_CONSTRAINT | (5<<8))
  SQLITE_CONSTRAINT_PRIMARYKEY        = (SQLITE_CONSTRAINT | (6<<8))
  SQLITE_CONSTRAINT_TRIGGER           = (SQLITE_CONSTRAINT | (7<<8))
  SQLITE_CONSTRAINT_UNIQUE            = (SQLITE_CONSTRAINT | (8<<8))
  SQLITE_CONSTRAINT_VTAB              = (SQLITE_CONSTRAINT | (9<<8))
  SQLITE_CONSTRAINT_ROWID             = (SQLITE_CONSTRAINT |(10<<8))
  SQLITE_CONSTRAINT_PINNED            = (SQLITE_CONSTRAINT |(11<<8))
  SQLITE_CONSTRAINT_DATATYPE          = (SQLITE_CONSTRAINT |(12<<8))
  SQLITE_NOTICE_RECOVER_WAL           = (SQLITE_NOTICE | (1<<8))
  SQLITE_NOTICE_RECOVER_ROLLBACK      = (SQLITE_NOTICE | (2<<8))
  SQLITE_WARNING_AUTOINDEX            = (SQLITE_WARNING | (1<<8))
  SQLITE_AUTH_USER                    = (SQLITE_AUTH | (1<<8))
  SQLITE_OK_LOAD_PERMANENTLY          = (SQLITE_OK | (1<<8))
  SQLITE_OK_SYMLINK                   = (SQLITE_OK | (2<<8))


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

  class Database
    # @!visibility private
    TABLES_SQL = (<<~SQL).freeze
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
      query_splat(format(TABLES_SQL, db: db))
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
      abort = false
      execute "begin #{mode} transaction"    
      yield self
    rescue => e
      abort = true
      e.is_a?(Rollback) ? nil : raise
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
      query_single_splat("pragma #{key}")
    end
  end

  class Query
    alias_method :execute_multi, :batch_execute
  end
end
