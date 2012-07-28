require 'java'
require 'memcached/version'
require 'memcached/exceptions'
require File.join(File.dirname(__FILE__), '../target/spymemcached-ext-0.0.1.jar')

class Memcached
  include_class 'java.net.InetSocketAddress'
  include_class 'net.spy.memcached.MemcachedClient'
  include_class 'net.spy.memcached.ConnectionFactoryBuilder'
  include_class 'net.spy.memcached.ConnectionFactoryBuilder$Locator'
  include_class 'net.spy.memcached.ConnectionFactoryBuilder$Protocol'
  include_class 'net.spy.memcached.DefaultHashAlgorithm'
  include_class 'net.spy.memcached.FailureMode'
  include_class 'net.spy.memcached.transcoders.SimpleTranscoder'
  include_class 'net.spy.memcached.AddrUtil'

  FLAGS = 0x0
  DEFAULTS = {
    :binary_protocol => false,
    :default_ttl => 604800,
    :distribution => :consistent_ketama,
    :exception_retry_limit => 5,
    :hash => :fnv1_32,
  }

  attr_reader :options
  attr_reader :default_ttl

  def initialize(addresses, options={})
    @options = DEFAULTS.merge(options)
    @default_ttl = @options[:default_ttl]

    builder = ConnectionFactoryBuilder.new

    @options[:distribution] = :consistent if @options[:distribution] == :ketama || @options[:distribution] == :consistent_ketama
    unless [:array_mod, :consistent].include? @options[:distribution]
      raise Memcached::NotSupport.new("#{@options[:distribution]} distribution algorithm doesn't support yet.")
    end
    distribution_algorithm = Locator.const_get @options[:distribution].to_s.upcase
    builder.setLocatorType(distribution_algorithm)

    if @options[:binary_protocol]
      builder.setProtocol(Protocol::BINARY)
    end

    unless [:native, :crc, :fnv1_64, :fnv1a_64, :fnv1_32, :fnv1a_32, :ketama].include? @options[:hash]
      raise Memcached::NotSupport.new("#{@options[:hash]} hash algorithm doesn't support yet.")
    end
    hash_algorithm = DefaultHashAlgorithm.const_get "#{@options[:hash]}_HASH".upcase
    builder.setHashAlg hash_algorithm

    @client = MemcachedClient.new builder.build, AddrUtil.getAddresses(Array(addresses).join(' '))

    @simple_transcoder = SimpleTranscoder.new
  end

  def set(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      @client.set(key, ttl, value.to_java_bytes, @simple_transcoder).get
    end
  end

  def add(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      if @client.add(key, ttl, value.to_java_bytes, @simple_transcoder).get === false
        raise Memcached::NotStored
      end
    end
  end

  def replace(key, value, ttl=@default_ttl, marshal=true, flags=FLAGS)
    with_retry do
      value = encode(value, marshal, flags)
      @simple_transcoder.setFlags(flags)
      if @client.replace(key, ttl, value.to_java_bytes, @simple_transcoder).get === false
        raise Memcached::NotStored
      end
    end
  end

  def delete(key)
    with_retry do
      raise Memcached::NotFound if @client.delete(key).get === false
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

  def flush
    @client.flush.get
  end

  def servers
    @client.available_servers.map { |address| address.to_s.split("/").last }
  end

  def close
    @client.shutdown
  end

  alias_method :quit, :close

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
