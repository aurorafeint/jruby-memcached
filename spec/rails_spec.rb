require 'spec_helper'

describe Memcached::Rails do
  context "localhost" do
    before(:all) { @memcached = Memcached::Rails.new(:servers => ["127.0.0.1:11211"]) }
    after(:all) { @memcached.shutdown }

    it "should get all servers" do
      @memcached.set "foo", "bar"
      @memcached.servers.should == ["127.0.0.1:11211"]
    end

    it "should get logger" do
      require 'logger'
      @memcached.logger = Logger.new(STDOUT)
      @memcached.logger.should_not be_nil
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

    context "get_multi" do
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

    context "set" do
      it "should set successfully" do
        @memcached.set("key", "value").should be_true
        @memcached.get("key").should == "value"
      end
    end

    context "write" do
      it "should write successfully" do
        @memcached.write("key", "value").should be_true
        @memcached.read("key").should == "value"
      end
    end

    context "fetch" do
      it "should read if key exists" do
        @memcached.write("key", "value")
        @memcached.fetch("key") { "new value" }.should == "value"
      end

      it "should write if key is missing" do
        @memcached.delete "key" rescue nil
        @memcached.fetch("key") { "new value" }.should == "new value"
      end
    end

    context "add" do
      it "should add if key is missing" do
        @memcached.delete "key" rescue nil
        @memcached.add("key", "value").should be_true
      end

      it "should do nothing if key exists" do
        @memcached.set "key", "value"
        @memcached.add("key", "value").should be_false
      end

      context "with string_return_types" do
        before(:all) { @string_memcached = Memcached::Rails.new("127.0.0.1:11211", :string_return_types => true) }
        after(:all) { @string_memcached.quit }

        it "should add if key is missing" do
          @string_memcached.delete "key" rescue nil
          @string_memcached.add("key", "value").should == "STORED\r\n"
        end

        it "should do nothing if key exists" do
          @string_memcached.set "key", "value"
          @string_memcached.add("key", "value").should == "NOT STORED\r\n"
        end
      end
    end

    context "delete" do
      it "should delete existing key" do
        @memcached.set "key", "value"
        @memcached.delete "key"
        @memcached.get("key").should be_nil
      end

      it "should do nothing if delete missing key" do
        @memcached.delete "key" rescue nil
        @memcached.delete "key"
        @memcached.get("key").should be_nil
      end
    end

    context "alias" do
      it "should respond_to? flush_all" do
        @memcached.should be_respond_to(:flush_all)
      end

      it "should respond_to? clear" do
        @memcached.should be_respond_to(:clear)
      end

      it "should respond_to? :[]" do
        @memcached.should be_respond_to(:"[]")
      end

      it "should respond_to? :[]=" do
        @memcached.should be_respond_to(:"[]=")
      end
    end

    context "read_multi" do
      it "should read hash containing multiple key/value pairs" do
        @memcached.write "key1", "value1"
        @memcached.write "key2", "value2"
        @memcached.read_multi(["key1", "key2"]).should == {"key1" => "value1", "key2" => "value2"}
      end

      it "should read empty hash without params" do
        @memcached.read_multi.should be_empty
      end
    end
  end
end
