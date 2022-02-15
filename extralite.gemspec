require_relative './lib/extralite/version'

Gem::Specification.new do |s|
  s.name        = 'extralite'
  s.version     = Extralite::VERSION
  s.licenses    = ['MIT']
  s.summary     = 'Extra-lightweight SQLite3 wrapper for Ruby'
  s.author      = 'Sharon Rosner'
  s.email       = 'sharon@noteflakes.com'
  s.files       = `git ls-files`.split
  s.homepage    = 'https://github.com/digital-fabric/extralite'
  s.metadata    = {
    "source_code_uri" => "https://github.com/digital-fabric/extralite",
    "documentation_uri" => "https://www.rubydoc.info/gems/extralite",
    "homepage_uri" => "https://github.com/digital-fabric/extralite",
    "changelog_uri" => "https://github.com/digital-fabric/extralite/blob/master/CHANGELOG.md"
  }
  s.rdoc_options = ["--title", "extralite", "--main", "README.md"]
  s.extra_rdoc_files = ["README.md"]
  s.extensions = ["ext/extralite/extconf.rb"]
  s.require_paths = ["lib"]
  s.required_ruby_version = '>= 2.7'

  s.add_development_dependency  'rake-compiler',        '1.1.6'
  s.add_development_dependency  'minitest',             '5.15.0'
  s.add_development_dependency  'simplecov',            '0.17.1'
  s.add_development_dependency  'yard',                 '0.9.27'

  s.add_development_dependency  'sequel',               '5.51.0'
end
