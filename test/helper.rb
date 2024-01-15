# frozen_string_literal: true

require 'bundler/setup'
require 'extralite'
require 'minitest/autorun'

puts "sqlite3 version: #{Extralite.sqlite3_version}"

IS_LINUX = RUBY_PLATFORM =~ /linux/
SKIP_RACTOR_TESTS = !IS_LINUX || (RUBY_VERSION =~ /^3\.0/)
