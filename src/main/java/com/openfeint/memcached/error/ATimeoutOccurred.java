package com.openfeint.memcached.error;

import org.jruby.anno.JRubyClass;

@JRubyClass(name="Memcached::ATimeoutOccurred", parent="Memcached::Error")
public class ATimeoutOccurred extends Error {
}
