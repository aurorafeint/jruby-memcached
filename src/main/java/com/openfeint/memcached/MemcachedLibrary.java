package com.openfeint.memcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyException;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

public class MemcachedLibrary implements Library {
    private Ruby ruby;

    public void load(final Ruby ruby, boolean bln) throws IOException {
        this.ruby = ruby;

        RubyClass memcached = ruby.defineClass("Memcached", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rc) {
                return new Memcached(ruby, rc);
            }
        });

        memcached.defineAnnotatedMethods(Memcached.class);

        RubyClass runtimeError = ruby.getRuntimeError();
        RubyClass memcachedError = memcached.defineClassUnder("Error", runtimeError, runtimeError.getAllocator());
        memcached.defineClassUnder("NotFound", memcachedError, memcachedError.getAllocator());
        memcached.defineClassUnder("NotStored", memcachedError, memcachedError.getAllocator());
        memcached.defineClassUnder("NotSupport", memcachedError, memcachedError.getAllocator());
    }
}
