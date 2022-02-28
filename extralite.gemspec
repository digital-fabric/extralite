require_relative './gemspec'

Gem::Specification.new do |s|
  common_spec(s)
  s.name        = 'extralite'
  s.summary     = 'Extra-lightweight SQLite3 wrapper for Ruby'
  s.extensions  = ["ext/extralite/extconf.rb"]
end
