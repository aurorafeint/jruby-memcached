package com.openfeint.memcached.error;

import org.jruby.anno.JRubyClass;

@JRubyClass(name="Memcached::NotSupport", parent="Memcached::Error")
public class NotSupport extends Error {
}
