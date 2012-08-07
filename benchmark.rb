require 'benchmark'

JRUBY = defined?(JRUBY_VERSION)

require 'rubygems'
if JRUBY
  require 'lib/memcached'
  require 'jruby-spymemcached'
else
  require 'memcached'
end
require 'dalli'

memcached = Memcached.new(['localhost:11211'])
spymemcached = Spymemcached.new(['localhost:11211']) if JRUBY
dalli = Dalli::Client.new(['localhost:11211'])

3.to_i.times {
  Benchmark.bm(30) {|bm|
    if JRUBY
      bm.report("jruby-memcached set") {
        100_000.times { memcached.set('foo', 'bar') }
      }
      bm.report("jruby-memcached get") {
        100_000.times { memcached.get('foo') }
      }
      bm.report("jruby-spymemcached set") {
        100_000.times { spymemcached.set('foo', 'bar') }
      }
      bm.report("jruby-spymemcached get") {
        100_000.times { spymemcached.get('foo') }
      }
    else
      bm.report("memcached set") {
        100_000.times { memcached.set('foo', 'bar') }
      }
      bm.report("memcached get") {
        100_000.times { memcached.get('foo') }
      }
    end
  }
}

3.times {
  Benchmark.bm(30) {|bm|
    bm.report("dalli set") {
      100_000.times { dalli.set('foo', 'bar') }
    }
    bm.report("dalli get") {
      100_000.times { dalli.get('foo') }
    }
  }
}

memcached.shutdown
spymemcached.shutdown if JRUBY
dalli.close

# MBP 2.8G i7    jruby-memcached 0.3.0
#
# ruby-1.9.3-p194
# ruby benchmark.rb
#                                      user     system      total        real
# memcached set                    1.110000   1.020000   2.130000 (  4.592509)
# memcached get                    0.970000   1.000000   1.970000 (  4.172170)
#                                      user     system      total        real
# dalli set                        8.360000   1.650000  10.010000 ( 10.193101)
# dalli get                        8.040000   1.670000   9.710000 (  9.828392)
#
# jruby-1.6.7.2
# jruby --server -Ilib -S benchmark.rb
#                                     user     system      total        real
# jruby-memcached set             5.842000   0.000000   5.842000 (  5.842000)
# jruby-memcached get             5.561000   0.000000   5.561000 (  5.561000)
#                                     user     system      total        real
# jruby-spymemcached set          5.919000   0.000000   5.919000 (  5.919000)
# jruby-spymemcached get          5.615000   0.000000   5.615000 (  5.615000)
#                                     user     system      total        real
# dalli set                      10.132000   0.000000  10.132000 ( 10.132000)
# dalli get                      10.600000   0.000000  10.600000 ( 10.600000)
#
##############################################################################
#
# MBP 2.8G i7    jruby-memcached 0.1.0
#
# ruby-1.9.3-p194
# ruby benchmark.rb
#                                      user     system      total        real
# memcached set                    1.110000   1.020000   2.130000 (  4.592509)
# memcached get                    0.970000   1.000000   1.970000 (  4.172170)
#                                      user     system      total        real
# dalli set                        8.330000   1.560000   9.890000 ( 10.094499)
# dalli get                        8.530000   1.680000  10.210000 ( 10.331083)
#
# jruby-1.6.7.2
# jruby --server -Ilib -S benchmark.rb
#                                     user     system      total        real
# jruby-memcached set             6.902000   0.000000   6.902000 (  6.902000)
# jruby-memcached get             6.845000   0.000000   6.845000 (  6.845000)
#                                     user     system      total        real
# dalli set                      13.251000   0.000000  13.251000 ( 13.251000)
# dalli get                      13.536000   0.000000  13.536000 ( 13.536000)
