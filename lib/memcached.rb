require 'java'
require 'memcached/version'
require File.join(File.dirname(__FILE__), '../target/spymemcached-ext-0.0.1.jar')
require 'com/openfeint/memcached/memcached'

class Memcached::Rails
  attr_reader :logger

  def logger=(logger)
    @logger = logger
  end
end
