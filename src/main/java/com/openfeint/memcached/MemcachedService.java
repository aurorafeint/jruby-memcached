package com.openfeint.memcached;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

import java.io.IOException;

public class MemcachedService implements BasicLibraryService {
    public boolean basicLoad(final Ruby ruby) throws IOException {
        RubyClass memcached = ruby.defineClass("Memcached", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass klazz) {
                return new Memcached(ruby, klazz);
            }
        });
        memcached.defineAnnotatedMethods(Memcached.class);
        memcached.defineClassUnder("Rails", memcached, memcached.getAllocator());

        RubyClass runtimeError = ruby.getRuntimeError();
        RubyClass memcachedError = memcached.defineClassUnder("Error", runtimeError, runtimeError.getAllocator());
        memcached.defineClassUnder("NotFound", memcachedError, memcachedError.getAllocator());
        memcached.defineClassUnder("NotStored", memcachedError, memcachedError.getAllocator());
        memcached.defineClassUnder("NotSupport", memcachedError, memcachedError.getAllocator());
        return true;
    }
}
