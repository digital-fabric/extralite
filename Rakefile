# frozen_string_literal: true

require 'bundler/gem_tasks'
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
