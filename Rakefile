#!/usr/bin/env rake
require "bundler/gem_tasks"

require "rake"
require "rspec"
require "rspec/core/rake_task"

$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)
require "memcached/version"

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = "spec/**/*_spec.rb"
end

task :default => :spec
task :test => :spec
