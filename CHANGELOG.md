## 0.5.0 (Aug 22, 2012)

Bugfixes:

  - fix Memcached increment/decrement, which works with MarshalTranscoder
now

Features:

  - update spymemcached to 2.8.3, which set shouldOptimize as false by
default
  - add Memcached::ATimeoutOccurred exception
  - accept exception_retry_limit option

## 0.4.1 (Aug 17, 2012)

Bugfixes:

  - fix ClassCastException from Long to RubyFixnum

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
