require_relative './extralite_ext'

# Extralite is a Ruby gem for working with SQLite databases
module Extralite
  
  SQLITE_STATUS_MEMORY_USED           =  0
  SQLITE_STATUS_PAGECACHE_USED        =  1
  SQLITE_STATUS_PAGECACHE_OVERFLOW    =  2
  SQLITE_STATUS_SCRATCH_USED          =  3 # NOT USED
  SQLITE_STATUS_SCRATCH_OVERFLOW      =  4 # NOT USED
  SQLITE_STATUS_MALLOC_SIZE           =  5
  SQLITE_STATUS_PARSER_STACK          =  6
  SQLITE_STATUS_PAGECACHE_SIZE        =  7
  SQLITE_STATUS_SCRATCH_SIZE          =  8 # NOT USED
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

  # The following class definitions are not really needed, as they're already
  # defined in the C extension. We put them here for the sake of generating
  # docs.

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
    alias_method :execute, :query

    TABLES_SQL = <<~SQL
      SELECT name FROM sqlite_master
      WHERE type ='table'
        AND name NOT LIKE 'sqlite_%';
    SQL

    def tables
      query_single_column(TABLES_SQL)
    end

    def pragma(value)
      value.is_a?(Hash) ? pragma_set(value) : pragma_get(value)
    end

    def pragma_set(values)
      sql = values.inject(+'') { |s, (k, v)| s += "pragma #{k}=#{v}; " }
      query(sql)
    end

    def pragma_get(key)
      query("pragma #{key}")
    end
  end

  # An SQLite backup
  class Backup
    # def initialize(dst, dst_name, src, src_name); end

    # def dst; end
    # def src; end

    # def step(pages); end
    # def finish; end

    # def pagecount; end
    # def remaining; end
  end
end
