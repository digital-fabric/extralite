# frozen_string_literal: true

require 'bundler/setup'
require 'extralite'
require 'minitest/autorun'
require 'tempfile'

puts "sqlite3 version: #{Extralite.sqlite3_version}"

IS_LINUX = RUBY_PLATFORM =~ /linux/
# Ractors are kinda flaky, there's no point in testing this
SKIP_RACTOR_TESTS = true #!IS_LINUX || (RUBY_VERSION =~ /^3\.[01]/)

module Minitest::Assertions
  def assert_in_range exp_range, act
    msg = message(msg) { "Expected #{mu_pp(act)} to be in range #{mu_pp(exp_range)}" }
    assert exp_range.include?(act), msg
  end
end

module TestExtensions
  def teardown
    @db = nil
    GC.start
    super
  ensure
    @tempfiles&.each(&:close!)
  end

  # Returns the path of a new temp file. The Tempfile is kept until teardown,
  # otherwise GC may delete the file while a database is still using it.
  def tempfile_path(name)
    (@tempfiles ||= []) << (tmp = Tempfile.new(name))
    tmp.path
  end
end

Minitest::Test.prepend(TestExtensions)
