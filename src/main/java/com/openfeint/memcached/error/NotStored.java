package com.openfeint.memcached.error;

import org.jruby.anno.JRubyClass;

@JRubyClass(name="Memcached::NotStored", parent="Memcached::Error")
public class NotStored extends Error {
}
