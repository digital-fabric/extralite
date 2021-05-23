# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/clean"

require "rake/extensiontask"
Rake::ExtensionTask.new("extralite_ext") do |ext|
  ext.ext_dir = "ext/extralite"
end

task :recompile => [:clean, :compile]

task :default => [:compile, :test]
task :test do
  exec 'ruby test/test_database.rb'
end

CLEAN.include "**/*.o", "**/*.so", "**/*.so.*", "**/*.a", "**/*.bundle", "**/*.jar", "pkg", "tmp"
