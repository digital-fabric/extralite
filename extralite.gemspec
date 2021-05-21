require_relative './lib/extralite/version'

Gem::Specification.new do |s|
  s.name        = 'extralite'
  s.version     = Extralite::VERSION
  s.licenses    = ['MIT']
  s.summary     = 'Extra-lightweight SQLite3 wrapper for Ruby'
  s.author      = 'Sharon Rosner'
  s.email       = 'sharon@noteflakes.com'
  s.files       = `git ls-files`.split
  s.homepage    = 'https://digital-fabric.github.io/extralite'
  s.metadata    = {
    "source_code_uri" => "https://github.com/digital-fabric/extralite",
    "documentation_uri" => "https://digital-fabric.github.io/extralite/",
    "homepage_uri" => "https://digital-fabric.github.io/extralite/",
    "changelog_uri" => "https://github.com/digital-fabric/extralite/blob/master/CHANGELOG.md"
  }
  s.rdoc_options = ["--title", "extralite", "--main", "README.md"]
  s.extra_rdoc_files = ["README.md"]
  s.extensions = ["ext/extralite/extconf.rb"]
  s.require_paths = ["lib"]
  s.required_ruby_version = '>= 2.6'

  s.add_development_dependency  'rake-compiler',        '1.1.1'
  s.add_development_dependency  'minitest',             '5.14.4'
  s.add_development_dependency  'minitest-reporters',   '1.4.2'
  s.add_development_dependency  'simplecov',            '0.17.1'
  s.add_development_dependency  'rubocop',              '0.85.1'
  s.add_development_dependency  'pry',                  '0.13.1'
end
