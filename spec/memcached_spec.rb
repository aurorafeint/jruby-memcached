require 'spec_helper'

describe Memcached do
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

    context "set/get" do
      it "should set/get with plain text" do
        @memcached.set "key", "value"
        @memcached.get("key").should == "value"
      end

      it "should set/get with compressed text" do
        @memcached.set "key", "x\234c?P?*?/?I\001\000\b8\002a"
        @memcached.get("key").should == "x\234c?P?*?/?I\001\000\b8\002a"
      end
    end

    context "get" do
      it "should get nil" do
        @memcached.set "key", nil, 0
        @memcached.get("key").should be_nil
      end

      it "should get missing" do
        @memcached.delete "key" rescue nil
        lambda { @memcached.get "key" }.should raise_error(Memcached::NotFound)
      end
    end

    context "set" do
      it "should set expiry" do
        @memcached.set "key", "value", 1
        @memcached.get("key").should == "value"
        sleep 1
        lambda { @memcached.get("key") }.should raise_error(Memcached::NotFound)
      end

      it "should retry when set failure" do
        Java::NetSpyMemcached::MemcachedClient.any_instance.stubs(:set).raises(Memcached::NotStored)
        Java::NetSpyMemcachedTranscoders::SimpleTranscoder.any_instance.expects(:setFlags).times(6)
        lambda { @memcached.set "key", "value" }.should raise_error(Memcached::NotStored)
      end
    end

    context "add" do
      it "should add new key" do
        @memcached.delete "key" rescue nil
        @memcached.add "key", "value"
        @memcached.get("key").should == "value"
      end

      it "should not add existing key" do
        @memcached.set "key", "value"
        lambda { @memcached.add "key", "value" }.should raise_error(Memcached::NotStored)
      end

      it "should add expiry" do
        @memcached.delete "key" rescue nil
        @memcached.add "key", "value", 1
        @memcached.get "key"
        sleep 1
        lambda { @memcached.get "key" }.should raise_error(Memcached::NotFound)
      end
    end

    context "replace" do
      it "should replace existing key" do
        @memcached.set "key", nil
        @memcached.replace "key", "value"
        @memcached.get("key").should == "value"
      end

      it "should not replace with new key" do
        @memcached.delete "key" rescue nil
        lambda { @memcached.replace "key", "value" }.should raise_error(Memcached::NotStored)
        lambda { @memcached.get "key" }.should raise_error(Memcached::NotFound)
      end
    end

    context "delete" do
      it "should delete with existing key" do
        @memcached.set "key", "value"
        @memcached.delete "key"
        lambda { @memcached.get "key" }.should raise_error(Memcached::NotFound)
      end

      it "should not delete with new key" do
        @memcached.delete "key" rescue nil
        lambda { @memcached.delete "key" }.should raise_error(Memcached::NotFound)
      end
    end

    context "flush" do
      it "should flush all keys" do
        @memcached.set "key1", "value2"
        @memcached.set "key2", "value2"
        @memcached.flush
        lambda { @memcached.get "key1" }.should raise_error(Memcached::NotFound)
        lambda { @memcached.get "key2" }.should raise_error(Memcached::NotFound)
      end
    end
  end
end
