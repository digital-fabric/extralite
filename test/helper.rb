# frozen_string_literal: true

require 'bundler/setup'
require 'extralite'
require 'minitest/autorun'

puts "sqlite3 version: #{Extralite.sqlite3_version}"