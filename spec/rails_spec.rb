require 'spec_helper'

describe Memcached::Rails do
  context "localhost" do
    before(:all) { @memcached = Memcached::Rails.new(["127.0.0.1:11211"]) }
    after(:all) { @memcached.shutdown }

    it "should get all servers" do
      @memcached.set "foo", "bar"
      @memcached.servers.should == ["127.0.0.1:11211"]
    end

    it "should be active" do
      @memcached.should be_active
    end

    context "get" do
      it "should get value" do
        @memcached.set "key", "value"
        @memcached.get("key").should == "value"
      end

      it "should get nil if key is missing" do
        @memcached.delete "key" rescue nil
        @memcached.get("key").should be_nil
      end
    end

    context "read" do
      it "should get value" do
        @memcached.set "key", "value"
        @memcached.get("key").should == "value"
      end

      it "should get nil if key is missing" do
        @memcached.delete "key" rescue nil
        @memcached.get("key").should be_nil
      end
    end

    context "exist?" do
      it "should return true if key exists" do
        @memcached.set "key", "value"
        @memcached.exist?("key").should be_true
      end

      it "should return false if key is missing" do
        @memcached.delete "key" rescue nil
        @memcached.exist?("key").should be_false
      end
    end
  end
end
