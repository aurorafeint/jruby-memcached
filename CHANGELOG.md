## 0.4.0 (Aug 16, 2012)

Bugfixes:

  - set as daemon thread to avoid suspend ruby process (like rake task)

Features:

  - support get with multiple keys
  - add Memcached::Rails as rails cache_store
  - use jruby annotation to reduce method definitions

## 0.3.0 (Aug 7, 2012)

Features:

  - rewrite with pure jruby implementation

## 0.2.0 (Jul 29, 2012)

Bugfixes:

  - set method should not be async

Features:

  - allow to change hash, distribution and binary protocol

## 0.1.0 (Jul 24, 2012)

Features:

  - wrap java library spymemcached
  - compatible hash and distribution algorithms with memcached.gem
