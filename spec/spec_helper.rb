require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'rspec/autorun'

require 'memcached'

RSpec.configure do |config|
  config.mock_framework = :mocha
end
