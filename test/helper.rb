# frozen_string_literal: true

require 'bundler/setup'
require 'extralite'
require 'minitest/autorun'
require 'minitest/reporters'

Minitest::Reporters.use! [
  Minitest::Reporters::SpecReporter.new
]
