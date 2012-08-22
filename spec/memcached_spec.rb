require 'spec_helper'

describe Memcached do
  context "localhost" do
    before(:all) { @memcached = Memcached.new(["127.0.0.1:11211"]) }
    after(:all) { @memcached.quit }

    it "should get all servers" do
      @memcached.set "foo", "bar"
      @memcached.servers.should == ["127.0.0.1:11211"]
    end

    context "initialize" do
      it "should connect to 127.0.0.1:11211 if no server defined" do
        memcached = Memcached.new
        memcached.set "foo", "bar"
        memcached.servers.should == ["127.0.0.1:11211"]
        memcached.shutdown
      end

      it "should raise error with unsupported option hash" do
        lambda { Memcached.new("127.0.0.1:11211", :hash => :unknown) }.should raise_error(Memcached::NotSupport)
      end

      it "should raise error with unsupported option distribution" do
        lambda { Memcached.new("127.0.0.1:11211", :distribution => :unknown) }.should raise_error(Memcached::NotSupport)
      end
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

      context "multiget" do
        it "should get hash containing multiple key/value pairs" do
          @memcached.set "key1", "value1"
          @memcached.set "key2", "value2"
          @memcached.get(["key1", "key2"]).should == {"key1" => "value1", "key2" => "value2"}
        end

        it "should get hash containing nil value" do
          @memcached.set "key", nil, 0
          @memcached.get(["key"]).should == {"key" => nil}
        end

        it "should get empty hash" do
          @memcached.delete "key" rescue nil
          @memcached.get(["key"]).should be_empty
        end
      end
    end

    context "set" do
      it "should set expiry" do
        @memcached.set "key", "value", 1
        @memcached.get("key").should == "value"
        sleep 1
        lambda { @memcached.get("key") }.should raise_error(Memcached::NotFound)
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

    context "increment" do
      it "should increment to default value" do
        @memcached.delete "intkey" rescue nil
        @memcached.increment "intkey"
        @memcached.get("intkey").should == 1
      end

      it "should increment by 1" do
        @memcached.delete "intkey" rescue nil
        @memcached.increment "intkey"
        @memcached.increment "intkey"
        @memcached.get("intkey").should == 2
      end

      it "should increment by 10" do
        @memcached.delete "intkey" rescue nil
        @memcached.increment "intkey"
        @memcached.increment "intkey", 10
        @memcached.get("intkey").should == 11
      end
    end

    context "decrement" do
      it "should decrement to default value" do
        @memcached.delete "intkey" rescue nil
        @memcached.decrement "intkey"
        @memcached.get("intkey").should == 0
      end

      it "should decrement by 1" do
        @memcached.delete "intkey" rescue nil
        2.times { @memcached.increment "intkey" }
        @memcached.decrement "intkey"
        @memcached.get("intkey").should == 1
      end

      it "should decrement by 10" do
        @memcached.delete "intkey" rescue nil
        @memcached.increment "intkey"
        @memcached.increment "intkey", 20
        @memcached.decrement "intkey", 10
        @memcached.get("intkey").should == 11
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

    context "namespace/prefix_key" do
      it "should get/set with namespace" do
        memcached = Memcached.new("127.0.0.1:11211", :namespace => "jruby")
        memcached.set "key", "value"
        memcached.get("key").should == "value"
        memcached.shutdown
        @memcached.get("jrubykey").should == "value"
      end

      context "prefix_key" do
        before(:all) { @prefix_memcached = Memcached.new("127.0.0.1:11211", :prefix_key => "jruby") }
        after(:all) { @prefix_memcached.shutdown }

        it "should get/set with prefix_key" do
          @prefix_memcached.set "key", "value"
          @prefix_memcached.get("key").should == "value"
          @memcached.get("jrubykey").should == "value"
        end

        it "should add/replace with prefix_key" do
          @prefix_memcached.add "newkey", "value"
          @prefix_memcached.replace "newkey", "new_value"
          @memcached.get("jrubynewkey").should == "new_value"
        end

        it "should delete with prefix_key" do
          @prefix_memcached.set "key", "value"
          @prefix_memcached.delete "key"
          lambda { @memcached.get("jrubykey") }.should raise_error(Memcached::NotFound)
        end

        it "should multiget with prefix_key" do
          @prefix_memcached.set "key1", "value1"
          @prefix_memcached.set "key2", "value2"
          @prefix_memcached.get(["key1", "key2"]).should == {"key1" => "value1", "key2" => "value2"}
        end

        it "should increment/decrement with prefix_key" do
          @prefix_memcached.delete "intkey" rescue nil
          @prefix_memcached.increment "intkey"
          @prefix_memcached.increment "intkey", 10
          @prefix_memcached.decrement "intkey", 5
          @memcached.get("jrubyintkey").should == 6
        end
      end
    end

    context "timeout" do
      before(:all) do
        @memcached = Memcached.new("127.0.0.1:11211")
        @timeout_memcached = Memcached.new("127.0.0.1:11211", :timeout => 1, :exception_retry_limit => 0)
      end
      after(:all) do
        @timeout_memcached.quit
        @memcached.quit
      end

      it "should set timeout" do
        lambda { @timeout_memcached.set "key", "new_value" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should add timeout" do
        @memcached.delete "key" rescue nil
        lambda { @timeout_memcached.add "key", "new_value" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should replace timeout" do
        @memcached.set "key", "value"
        lambda { @timeout_memcached.replace "key", "new_value" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should delete timeout" do
        @memcached.set "key", "value"
        lambda { @timeout_memcached.delete "key" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should get timeout" do
        @memcached.set "key", "value"
        lambda { @timeout_memcached.get "key" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should increment timeout" do
        lambda { @timeout_memcached.increment "intkey" }.should raise_error(Memcached::ATimeoutOccurred)
      end

      it "should decrement timeout" do
        lambda { @timeout_memcached.decrement "intkey" }.should raise_error(Memcached::ATimeoutOccurred)
      end
    end
  end
end
