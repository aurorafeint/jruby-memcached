package com.openfeint.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyException;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

public class Memcached extends RubyObject {
    private Ruby ruby;

    private MemcachedClient client;

    private Transcoder<IRubyObject> transcoder;

    private int ttl;

    public Memcached(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);
        this.ruby = ruby;
    }

    @JRubyMethod
    public IRubyObject initialize(ThreadContext context, IRubyObject servers) {
        return initialize(context, servers, context.nil);
    }
    @JRubyMethod
    public IRubyObject initialize(ThreadContext context, IRubyObject servers, IRubyObject options) {
        List<InetSocketAddress> addresses;
        if ("java.lang.String".equals(servers.getJavaClass().getName())) {
            addresses = AddrUtil.getAddresses(servers.convertToString().toString());
        } else {
            addresses = AddrUtil.getAddresses((List<String>)servers.convertToArray());
        }
        try {
            ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

            String distributionValue = null;
            String hashValue = null;
            String binaryValue = null;
            String defaultTTL = null;
            String transcoderValue = null;
            if (!options.isNil()) {
              RubyHash opts = options.convertToHash();
              if (opts.containsKey(ruby.newSymbol("distribution"))) {
                distributionValue = opts.get(ruby.newSymbol("distribution")).toString();
              }
              if (opts.containsKey(ruby.newSymbol("hash"))) {
                hashValue = opts.get(ruby.newSymbol("hash")).toString();
              }
              if (opts.containsKey(ruby.newSymbol("binary_protocol"))) {
                binaryValue = opts.get(ruby.newSymbol("binary_protocol")).toString();
              }
              if (opts.containsKey(ruby.newSymbol("default_ttl"))) {
                defaultTTL = opts.get(ruby.newSymbol("default_ttl")).toString();
              }
              if (opts.containsKey(ruby.newSymbol("transcoder"))) {
                transcoderValue = opts.get(ruby.newSymbol("transcoder")).toString();
              }
            }

            if (distributionValue == null) {
                distributionValue = "ketama";
            }
            if ("array_mod".equals(distributionValue)) {
                builder.setLocatorType(Locator.ARRAY_MOD);
            } else if ("ketama".equals(distributionValue) || "consistent_ketama".equals(distributionValue)) {
                builder.setLocatorType(Locator.CONSISTENT);
            } else {
                throw newNotSupport(ruby, "distribution not support");
            }

            if (hashValue == null) {
                hashValue = "fnv1_32";
            }
            if ("native".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.NATIVE_HASH);
            } else if ("crc".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.CRC_HASH);
            } else if ("fnv1_64".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1_64_HASH);
            } else if ("fnv1a_64".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH);
            } else if ("fnv1_32".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1_32_HASH);
            } else if ("fnv1a_32".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.FNV1A_32_HASH);
            } else if ("ketama".equals(hashValue)) {
                builder.setHashAlg(DefaultHashAlgorithm.KETAMA_HASH);
            } else {
                throw newNotSupport(ruby, "hash not support");
            }

            if ("true".equals(binaryValue)) {
                builder.setProtocol(Protocol.BINARY);
            }

            if (defaultTTL == null) {
                ttl = 604800;
            } else {
                ttl = Integer.parseInt(defaultTTL);
            }

            client = new MemcachedClient(builder.build(), addresses);

            if ("openfeint".equals(transcoderValue)) {
              transcoder = new OpenfeintTranscoder(ruby);
            } else {
              transcoder = new SimpleTranscoder(ruby);
            }
        } catch (IOException ioe) {
            throw context.runtime.newIOErrorFromException(ioe);
        }

        return context.nil;
    }

    @JRubyMethod
    public IRubyObject servers(ThreadContext context) {
        List<IRubyObject> addresses = new ArrayList<IRubyObject>();
        for (SocketAddress address : client.getAvailableServers()) {
            String addressStr = address.toString();
            if (addressStr.indexOf("/") == 0) {
                addressStr = addressStr.replace("/", "");
            }
            addresses.add(ruby.newString(addressStr));
        }
        return ruby.newArray(addresses);
    }

    @JRubyMethod
    public IRubyObject add(ThreadContext context, IRubyObject key, IRubyObject value) {
        return add(context, key, value, ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject add(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
        try {
            boolean result = client.add(key.toString(), (int)timeout.convertToInteger().getLongValue(), value, transcoder).get();
            if (result == false) {
                throw newNotStored(ruby, "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject replace(ThreadContext context, IRubyObject key, IRubyObject value) {
        return replace(context, key, value, ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject replace(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
        try {
            boolean result = client.replace(key.toString(), (int)timeout.convertToInteger().getLongValue(), value, transcoder).get();
            if (result == false) {
                throw newNotStored(ruby, "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value) {
        return set(context, key, value, ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
        try {
            boolean result = client.set(key.toString(), (int)timeout.convertToInteger().getLongValue(), value, transcoder).get();
            if (result == false) {
                throw newNotStored(ruby, "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject get(ThreadContext context, IRubyObject key) {
        IRubyObject value = client.get(key.toString(), transcoder);
        if (value == null) {
          throw newNotFound(ruby, "not found");
        }
        return value;
    }

    @JRubyMethod
    public  IRubyObject incr(ThreadContext context, IRubyObject key) {
        return incr(context, key, ruby.newFixnum(1), ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject incr(ThreadContext context, IRubyObject key, IRubyObject by) {
        return incr(context, key, by, ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject incr(ThreadContext context, IRubyObject key, IRubyObject by, IRubyObject timeout) {
        long result = client.incr(key.toString(), (int)by.convertToInteger().getLongValue(), 1, (int)timeout.convertToInteger().getLongValue());
        return ruby.newFixnum(result);
    }

    @JRubyMethod
    public IRubyObject decr(ThreadContext context, IRubyObject key) {
        return decr(context, key, ruby.newFixnum(1), ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject decr(ThreadContext context, IRubyObject key, IRubyObject by) {
        return decr(context, key, by, ruby.newFixnum(ttl));
    }

    @JRubyMethod
    public IRubyObject decr(ThreadContext context, IRubyObject key, IRubyObject by, IRubyObject timeout) {
        long result = client.decr(key.toString(), (int)by.convertToInteger().getLongValue(), 0, (int)timeout.convertToInteger().getLongValue());
        return ruby.newFixnum(result);
    }

    @JRubyMethod
    public IRubyObject delete(ThreadContext context, IRubyObject key) {
        try {
            boolean result = client.delete(key.toString()).get();
            if (result == false) {
                throw newNotFound(ruby, "not found");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject flush(ThreadContext context) {
        try {
            client.flush().get();
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject stats(ThreadContext context) {
        RubyHash results = RubyHash.newHash(ruby);
        for(Map.Entry<SocketAddress, Map<String, String>> entry : client.getStats().entrySet()) {
            RubyHash serverHash = RubyHash.newHash(ruby);
            for(Map.Entry<String, String> server : entry.getValue().entrySet()) {
                serverHash.op_aset(context, ruby.newString(server.getKey()), ruby.newString(server.getValue()));
            }
            results.op_aset(context, ruby.newString(entry.getKey().toString()), serverHash);
        }
        return results;
    }

    @JRubyMethod
    public IRubyObject shutdown(ThreadContext context) {
        client.shutdown();

        return context.nil;
    }

    @JRubyClass(name="Memcached::Error", parent="RuntimeError")
    public static class Error {}
    @JRubyClass(name="Memcached::NotFound", parent="Memcached::Error")
    public static class NotFound extends Error {}
    @JRubyClass(name="Memcached::NotStored", parent="Memcached::Error")
    public static class NotStored extends Error {}
    @JRubyClass(name="Memcached::NotSupport", parent="Memcached::Error")
    public static class NotSupport extends Error {}

    static RaiseException newNotFound(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotFound", message);
    }

    static RaiseException newNotStored(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotStored", message);
    }

    static RaiseException newNotSupport(Ruby ruby, String message) {
        return newMemcachedError(ruby, "NotSupport", message);
    }

    private static RaiseException newMemcachedError(Ruby ruby, String klass, String message) {
        RubyClass errorClass = ruby.getModule("Memcached").getClass(klass);
        return new RaiseException(RubyException.newException(ruby, errorClass, message), true);
    }
}
