class Memcached::Error < RuntimeError; end
class Memcached::NotFound < Memcached::Error; end
class Memcached::NotStored < Memcached::Error; end
