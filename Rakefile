# frozen_string_literal: true

require 'rake/clean'

require 'rake/extensiontask'
Rake::ExtensionTask.new('extralite_ext') do |ext|
  ext.ext_dir = 'ext/extralite'
end

task :recompile => [:clean, :compile]

task :default => [:compile, :doc, :test]
task :doc => :yard
task :test do
  exec 'ruby test/run.rb'
end

CLEAN.include 'lib/*.o', 'lib/*.so', 'lib/*.so.*', 'lib/*.a', 'lib/*.bundle', 'lib/*.jar', 'pkg', 'tmp'

require 'yard'
YARD_FILES = FileList['ext/extralite/extralite.c', 'lib/extralite.rb', 'lib/sequel/adapters/extralite.rb']

YARD::Rake::YardocTask.new do |t|
  t.files   = YARD_FILES
  t.options = %w(-o doc --readme README.md)
end

task :release do
  require_relative './lib/extralite/version'
  version = Extralite::VERSION
  
  puts 'Building extralite...'
  `gem build extralite.gemspec`

  puts 'Building extralite-bundle...'
  `gem build extralite-bundle.gemspec`

  puts "Pushing extralite #{version}..."
  `gem push extralite-#{version}.gem`

  puts "Pushing extralite-bundle #{version}..."
  `gem push extralite-bundle-#{version}.gem`

  puts "Cleaning up..."
  `rm *.gem`
end

task :build_bundled do
  puts 'Building extralite-bundle...'
  `gem build extralite-bundle.gemspec`
end

test_config = lambda do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/test_*.rb"]
end

# Rake::TestTask.new(:test, &test_config)

begin
  require "ruby_memcheck"

  namespace :test do
    RubyMemcheck::TestTask.new(:valgrind, &test_config)
  end
rescue LoadError => e
  warn("NOTE: ruby_memcheck is not available in this environment: #{e}")
end