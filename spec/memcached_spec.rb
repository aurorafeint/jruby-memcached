require 'spec_helper'

describe Memcached do
  context "stub memcached" do
    before { Java::NetSpyMemcached::MemcachedClient.expects(:new) }
    subject { Memcached.new(["10.0.0.1:11211", "10.0.0.2:11211"], :default_ttl => 100) }
    its(:servers) { should == ["10.0.0.1:11211", "10.0.0.2:11211"] }
    its(:default_ttl) { should == 100 }
  end

  context "localhost" do
    before :all do
      @memcached = Memcached.new("127.0.0.1:11211")
    end

    after :all do
      @memcached.close
    end

    it "should initialize with options" do
      @memcached.servers.should == ["127.0.0.1:11211"]
    end

    it "should set/get with plain text" do
      @memcached.set "key", "value"
      @memcached.get("key").should == "value"
    end

    it "should set/get with compressed text" do
      @memcached.set "key", "x\234c?P?*?/?I\001\000\b8\002a"
      @memcached.get("key").should == "x\234c?P?*?/?I\001\000\b8\002a"
    end

    it "should get nil" do
      @memcached.set "key", nil, 0
      @memcached.get("key").should be_nil
    end

    it "should get missing" do
      @memcached.delete "key" rescue nil
      lambda { @memcached.get "key" }.should raise_error(Memcached::NotFound)
    end

    it "should set expiry" do
      @memcached.set "key", "value", 1
      @memcached.get("key").should == "value"
      sleep 1
      lambda { @memcached.get("key") }.should raise_error(Memcached::NotFound)
    end

    it "should retry when set failure" do
      Java::NetSpyMemcached::MemcachedClient.any_instance.stubs(:set).raises(Memcached::NotStored)
      Java::ComOpenfeintMemcachedTranscoders::SimpleTranscoder.any_instance.expects(:setFlags).times(6)
      lambda { @memcached.set "key", "value" }.should raise_error(Memcached::NotStored)
    end
  end
end
