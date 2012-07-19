require 'java'
require 'memcached/version'
require 'java/spymemcached-2.8.2-SNAPSHOT.jar'

class Memcached
  include_class 'java.net.InetSocketAddress'
  include_class 'net.spy.memcached.MemcachedClient'
  include_class 'net.spy.memcached.ConnectionFactoryBuilder'
  include_class 'net.spy.memcached.ConnectionFactoryBuilder$Locator'
  include_class 'net.spy.memcached.DefaultHashAlgorithm'

  attr_reader :default_ttl

  def initialize(addresses, options={})
    @servers = Array(addresses).map do |address|
      host, port = address.split(":")
      InetSocketAddress.new host, port.to_i
    end
    builder = ConnectionFactoryBuilder.new.setLocatorType(Locator::CONSISTENT).setHashAlg(DefaultHashAlgorithm::FNV1_32_HASH)
    # jruby is not smart enough to use MemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs)
    @client = MemcachedClient.new @servers
    # MemcachedClient has no interface to set connFactory, has to do manually
    @client.instance_variable_set :@connFactory, builder

    @default_ttl = options[:default_ttl] || 7 * 3600 # 7 days
  end

  def set(key, value, ttl=@default_ttl, marshal=true)
    value = marshal ? Marshal.dump(value) : value
    @client.set(key, ttl, value)
  end

  def get(key, marshal=true)
    value = @client.get(key)
    marshal ? Marshal.load(value) : value
  end

  def servers
    @servers.map { |server| server.to_s[1..-1] }
  end

  def close
    @client.shutdown
  end
end
