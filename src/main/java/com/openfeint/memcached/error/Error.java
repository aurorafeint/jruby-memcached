package com.openfeint.memcached.error;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyException;
import org.jruby.anno.JRubyClass;
import org.jruby.exceptions.RaiseException;

@JRubyClass(name = "Memcached::Error", parent = "RuntimeError")
public class Error {
    public static RaiseException newNotFound(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotFound", message);
    }

    public static RaiseException newNotStored(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotStored", message);
    }

    public static RaiseException newNotSupport(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotSupport", message);
    }

    private static RaiseException newMemcachedError(Ruby ruby, String klass, String message) {
        RubyClass errorClass = ruby.getModule("Memcached").getClass(klass);
        return new RaiseException(RubyException.newException(ruby, errorClass, message), true);
    }
}
