require_relative './gemspec'

Gem::Specification.new do |s|
  common_spec(s)

  s.name        = 'extralite-bundle'
  s.summary     = 'Extra-lightweight SQLite3 wrapper for Ruby with bundled SQLite3'
  s.extensions  = ["ext/extralite/extconf-bundle.rb"]
end
