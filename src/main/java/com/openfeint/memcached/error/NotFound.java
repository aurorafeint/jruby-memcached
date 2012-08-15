package com.openfeint.memcached.error;

import org.jruby.anno.JRubyClass;

@JRubyClass(name="Memcached::NotFound", parent="Memcached::Error")
public class NotFound extends Error {
}
