# frozen_string_literal: true

require 'bundler/setup'
require 'extralite'
require 'minitest/autorun'

puts "sqlite3 version: #{Extralite.sqlite3_version}"

IS_LINUX = RUBY_PLATFORM =~ /linux/
SKIP_RACTOR_TESTS = !IS_LINUX || (RUBY_VERSION =~ /^3\.[01]/)

module Minitest::Assertions
  def assert_in_range exp_range, act
    msg = message(msg) { "Expected #{mu_pp(act)} to be in range #{mu_pp(exp_range)}" }
    assert exp_range.include?(act), msg
  end
end
