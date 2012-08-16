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
  end
end
