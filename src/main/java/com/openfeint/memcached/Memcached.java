package com.openfeint.memcached;

import com.openfeint.memcached.error.Error;
import com.openfeint.memcached.transcoder.MarshalTranscoder;
import com.openfeint.memcached.transcoder.MarshalZlibTranscoder;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@JRubyClass(name = "Memcached")
public class Memcached extends RubyObject {
    private Ruby ruby;

    private MemcachedClient client;

    private Transcoder<IRubyObject> transcoder;

    private int ttl;

    public Memcached(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);
        this.ruby = ruby;
    }

    @JRubyMethod(name = "initialize", optional = 2)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        RubyHash options;
        if (args.length > 1) {
            options = args[1].convertToHash();
        } else {
            options = new RubyHash(ruby);
        }
        List<String> servers = new ArrayList<String>();
        if (args.length > 0) {
            if (args[0] instanceof RubyString) {
                servers.add(args[0].convertToString().toString());
            } else if (args[0] instanceof RubyArray) {
                servers.addAll((List<String>) args[0].convertToArray());
            }
        } else {
            servers.add("127.0.0.1:11211");
        }
        List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
        try {
            ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

            String distributionValue = null;
            String hashValue = null;
            String binaryValue = null;
            String defaultTTL = null;
            String transcoderValue = null;
            if (!options.isEmpty()) {
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
                throw Error.newNotSupport(ruby, "distribution not support");
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
                throw Error.newNotSupport(ruby, "hash not support");
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

            if ("marshal_zlib".equals(transcoderValue)) {
              transcoder = new MarshalZlibTranscoder(ruby);
            } else {
              transcoder = new MarshalTranscoder(ruby);
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

    @JRubyMethod(name = "add", required = 2, optional = 3)
    public IRubyObject add(ThreadContext context, IRubyObject[] args) {
        RubyString key = args[0].convertToString();
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.add(key.toString(), timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(ruby, "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod(name = "replace", required = 2, optional = 3)
    public IRubyObject replace(ThreadContext context, IRubyObject [] args) {
        RubyString key = args[0].convertToString();
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.replace(key.toString(), timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(ruby, "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod(name = "set", required = 2, optional = 3)
    public IRubyObject set(ThreadContext context, IRubyObject[] args) {
        RubyString key = args[0].convertToString();
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.set(key.toString(), timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(ruby, "not stored");
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
          throw Error.newNotFound(ruby, "not found");
        }
        return value;
    }

    @JRubyMethod
    public IRubyObject multiget(ThreadContext context, IRubyObject keys) {
        RubyHash results = RubyHash.newHash(ruby);

        Map<String, IRubyObject> bulkResults = client.getBulk(keys.convertToArray(), transcoder);
        for (Map.Entry<String, IRubyObject> entry : bulkResults.entrySet()) {
            results.op_aset(context, ruby.newString(entry.getKey()), entry.getValue());
        }
        return results;
    }

    @JRubyMethod(name = "incr", required = 1, optional = 2)
    public IRubyObject incr(ThreadContext context, IRubyObject[] args) {
        RubyString key = args[0].convertToString();
        int by = getIncrDecrBy(args);
        int timeout = getTimeout(args);
        long result = client.incr(key.toString(), by, 1, timeout);
        return ruby.newFixnum(result);
    }

    @JRubyMethod(name = "decr", required = 1, optional = 2)
    public IRubyObject decr(ThreadContext context, IRubyObject[] args) {
        RubyString key = args[0].convertToString();
        int by = getIncrDecrBy(args);
        int timeout = getTimeout(args);
        long result = client.decr(key.toString(), by, 0, timeout);
        return ruby.newFixnum(result);
    }

    @JRubyMethod
    public IRubyObject delete(ThreadContext context, IRubyObject key) {
        try {
            boolean result = client.delete(key.toString()).get();
            if (result == false) {
                throw Error.newNotFound(ruby, "not found");
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

    private int getTimeout(IRubyObject[] args) {
        if (args.length > 2) {
          return (int) args[2].convertToInteger().getLongValue();
        }
        return ttl;
    }

    private int getIncrDecrBy(IRubyObject[] args) {
        if (args.length > 1) {
            return (int) args[1].convertToInteger().getLongValue();
        }
        return 1;
    }
}
