require 'benchmark'

JRUBY = defined?(JRUBY_VERSION)

if JRUBY
  require 'lib/memcached'
else
  require 'memcached'
end
require 'rubygems'
require 'dalli'

memcached = Memcached.new(['localhost:11211'])
dalli = Dalli::Client.new(['localhost:11211'])

3.to_i.times {
  Benchmark.bm(30) {|bm|
    if JRUBY
      bm.report("jruby-memcached set") {
        100_000.times { memcached.set('foo', 'bar').get }
      }
      bm.report("jruby-memcached get") {
        100_000.times { memcached.get('foo') }
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

memcached.close
dalli.close


# MBP 2.8G i7
#
# ruby-1.9.3-p194
# ruby benchmark.rb
#                                      user     system      total        real
# memcached set                    1.110000   1.010000   2.120000 (  4.578121)
# memcached get                    0.940000   0.960000   1.900000 (  4.013941)
#                                      user     system      total        real
# memcached set                    1.100000   1.010000   2.110000 (  4.557462)
# memcached get                    0.930000   0.950000   1.880000 (  3.995192)
#                                      user     system      total        real
# memcached set                    1.110000   1.020000   2.130000 (  4.592509)
# memcached get                    0.970000   1.000000   1.970000 (  4.172170)
#                                      user     system      total        real
# dalli set                        8.330000   1.570000   9.900000 ( 10.062159)
# dalli get                        8.240000   1.630000   9.870000 (  9.987921)
#                                      user     system      total        real
# dalli set                        8.400000   1.580000   9.980000 ( 10.139169)
# dalli get                        8.500000   1.680000  10.180000 ( 10.287153)
#                                      user     system      total        real
# dalli set                        8.330000   1.560000   9.890000 ( 10.094499)
# dalli get                        8.530000   1.680000  10.210000 ( 10.331083)
#
# jruby-1.6.7.2
# jruby --server -Ilib -S benchmark.rb
#                                     user     system      total        real
# jruby-memcached set             8.716000   0.000000   8.716000 (  8.716000)
# jruby-memcached get             7.401000   0.000000   7.401000 (  7.402000)
#                                     user     system      total        real
# jruby-memcached set             6.530000   0.000000   6.530000 (  6.530000)
# jruby-memcached get             6.435000   0.000000   6.435000 (  6.435000)
#                                     user     system      total        real
# jruby-memcached set             6.471000   0.000000   6.471000 (  6.471000)
# jruby-memcached get             6.413000   0.000000   6.413000 (  6.413000)
#                                     user     system      total        real
# dalli set                      14.617000   0.000000  14.617000 ( 14.617000)
# dalli get                      13.197000   0.000000  13.197000 ( 13.197000)
#                                     user     system      total        real
# dalli set                      12.337000   0.000000  12.337000 ( 12.337000)
# dalli get                      12.699000   0.000000  12.699000 ( 12.699000)
#                                     user     system      total        real
# dalli set                      12.290000   0.000000  12.290000 ( 12.290000)
# dalli get                      12.823000   0.000000  12.823000 ( 12.823000)
