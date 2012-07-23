require 'java'
require 'memcached/version'
require 'memcached/exceptions'
require File.join(File.dirname(__FILE__), '../target/xmemcached-ext-0.0.1.jar')

class Memcached
  include_class 'net.rubyeye.xmemcached.XMemcachedClientBuilder'
  include_class 'net.rubyeye.xmemcached.utils.AddrUtil'
  include_class 'net.rubyeye.xmemcached.impl.LibmemcachedMemcachedSessionLocator'
  include_class 'net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator'
  include_class 'net.rubyeye.xmemcached.HashAlgorithm'
  include_class 'com.openfeint.memcached.transcoders.SimpleTranscoder'

  FLAGS = 0x0
  DEFAULTS = {
    :default_ttl => 604800,
    :exception_retry_limit => 5
  }

  attr_reader :options
  attr_reader :default_ttl

  def initialize(addresses, options={})
    builder = XMemcachedClientBuilder.new AddrUtil.getAddresses(Array(addresses).join(' '))
    builder.setSessionLocator(LibmemcachedMemcachedSessionLocator.new(100, HashAlgorithm::FNV1_32_HASH))
    @client = builder.build

    @options = DEFAULTS.merge(options)

    @default_ttl = @options[:default_ttl]
    @flags = @options[:flags]

    @simple_transcoder = SimpleTranscoder.new
  end

  def set(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      @client.set(key, ttl, value.to_java_bytes, @simple_transcoder)
    end
  end

  def add(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      if @client.add(key, ttl, value.to_java_bytes, @simple_transcoder) === false
        raise Memcached::NotStored
      end
    end
  end

  def replace(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      if @client.replace(key, ttl, value.to_java_bytes, @simple_transcoder) === false
        raise Memcached::NotStored
      end
    end
  end

  def delete(key)
    with_retry do
      raise Memcached::NotFound if @client.delete(key) === false
    end
  end

  def get(key, marshal=true)
    with_retry do
      ret = @client.get(key, @simple_transcoder)
      raise Memcached::NotFound if ret.nil?
      flags, data = ret.flags, ret.data
      value = String.from_java_bytes data
      value = decode(value, marshal, flags)
    end
  end

  def close
    @client.shutdown
  end

  private
  def with_retry
    begin
      yield
    rescue
      tries ||= 0
      raise unless tries < options[:exception_retry_limit]
      tries += 1
      retry
    end
  end

  def encode(value, marshal, flags)
    marshal ? Marshal.dump(value) : value
  end

  def decode(value, marshal, flags)
    marshal ? Marshal.load(value) : value
  end
end
