# jruby-memcached

A jruby memcached gem which is compatible with evan's [memcached][0] gem

## Install

    gem install jruby-memcached

## Usage

Now, in Ruby, require the library and instantiate a Memcached object at
a global level:

    require 'memcached'
    $cache = Memcached.new("localhost:11211")

Now you can set things and get things:

    value = 'hello'
    $cache.set 'test', value
    $cache.get 'test' #=> "hello"

You can set with an expiration timeout:

    value = 'hello'
    $cache.set 'test', value, 1
    sleep(2)
    $cache.get 'test' #=> raises Memcached::NotFound

You can get multiple values at once:

    value = 'hello'
    $cache.set 'test', value
    $cache.set 'test2', value
    $cache.get ['test', 'test2', 'missing']
      #=> {"test" => "hello", "test2" => "hello"}

## Benchmarks

memcached.gem is the fastest memcached gem in MRI,
jruby-memcached is the fastest memcached gem in JRuby.
See [benchmark][1]

[0]:"https://github.com/evan/memcached"
[1]:"https://github.com/aurorafeint/jruby-memcached/blob/master/benchmark.rb"
