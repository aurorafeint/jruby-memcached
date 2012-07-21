require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'rspec/autorun'

require 'memcached'

RSpec.configure do |config|
  config.mock_framework = :mocha
  config.filter_run :focus => true
  config.run_all_when_everything_filtered = true
end
