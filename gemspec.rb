require_relative './lib/extralite/version'

def common_spec(s)
  s.version     = Extralite::VERSION
  s.licenses    = ['MIT']
  s.author      = 'Sharon Rosner'
  s.email       = 'sharon@noteflakes.com'
  s.files       = `git ls-files`.split
  s.homepage    = 'https://github.com/digital-fabric/extralite'
  s.metadata    = {
    'homepage_uri' => 'https://github.com/digital-fabric/extralite',
    'documentation_uri' => 'https://www.rubydoc.info/gems/extralite',
    'changelog_uri' => 'https://github.com/digital-fabric/extralite/blob/master/CHANGELOG.md'
  }
  s.rdoc_options = ['--title', 'Extralite', '--main', 'README.md']
  s.extra_rdoc_files = ['README.md']
  s.require_paths = ['lib']
  s.required_ruby_version = '>= 3.0'

  s.add_development_dependency  'rake-compiler',        '1.2.7'
  s.add_development_dependency  'minitest',             '5.21.2'
  s.add_development_dependency  'simplecov',            '0.17.1'
  s.add_development_dependency  'yard',                 '0.9.34'
  s.add_development_dependency  'sequel',               '5.77.0'
end
