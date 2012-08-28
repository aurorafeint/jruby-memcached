require 'spec_helper'

# This is a stupid way to test timeout cases.
#
# Originally, I want to mock MemcachedClient methods to throw OperationTimeout, like
#
#     MemcachedClient.any_instance.stubs(:get).raises(OperationTimeout)
#     lambda { @timeout_memcached.get "key" }.should raise_error(Memcached::ATimeoutOccurred)
#
# But it doesn't work.
#
# The current solution is to call @timeout_memcahced.get in an infinite loop until a
# Memcached::ATimeoutOccurred raised. It works, but really slow down the tests, I hate it.
#
# If you have a better solution, please let me know flyerhzm at gmail dot com, thanks.
describe Memcached do
  before(:all) do
    @memcached = Memcached.new("127.0.0.1:11211")
    @timeout_memcached = Memcached.new("127.0.0.1:11211", :timeout => 1, :exception_retry_limit => 0)
  end

  after(:all) do
    @timeout_memcached.quit
    @memcached.quit
  end

  it "should set timeout" do
    lambda {
      while true
        @timeout_memcached.set "key", "new_value"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should add timeout" do
    lambda {
      while true
        @memcached.delete "key" rescue nil
        @timeout_memcached.add "key", "new_value"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should replace timeout" do
    @memcached.set "key", "value"
    lambda {
      while true
        @timeout_memcached.replace "key", "new_value"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should delete timeout" do
    lambda {
      while true
        @memcached.set "key", "value"
        @timeout_memcached.delete "key"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should get timeout" do
    @memcached.set "key", "value"
    lambda {
      while true
        @timeout_memcached.get "key"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should increment timeout" do
    lambda {
      while true
        @timeout_memcached.increment "intkey"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end

  it "should decrement timeout" do
    lambda {
      while true
        @timeout_memcached.decrement "intkey"
      end
    }.should raise_error(Memcached::ATimeoutOccurred)
  end
end
