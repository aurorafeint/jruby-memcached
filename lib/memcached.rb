require 'java'
require 'memcached/version'
require File.join(File.dirname(__FILE__), '../target/spymemcached-ext-0.0.1.jar')

com.openfeint.memcached.MemcachedLibrary.new.load(JRuby.runtime, false)
