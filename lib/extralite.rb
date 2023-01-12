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
