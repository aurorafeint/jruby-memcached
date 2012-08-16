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
    private MemcachedClient client;

    private Transcoder<IRubyObject> transcoder;

    private int ttl;

    private String prefixKey;

    public Memcached(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);

        ttl = 604800;
        prefixKey = "";
    }

    @JRubyMethod(name = "initialize", optional = 2)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        RubyHash options;
        if (args.length > 1) {
            options = args[1].convertToHash();
        } else {
            options = new RubyHash(getRuntime());
        }
        List<String> servers = new ArrayList<String>();
        if (args.length > 0) {
            if (args[0] instanceof RubyString) {
                servers.add(args[0].toString());
            } else if (args[0] instanceof RubyArray) {
                servers.addAll((List<String>) args[0].convertToArray());
            }
        }
        if (servers.isEmpty()) {
            servers.add("127.0.0.1:11211");
        }
        return init(context, servers, options);
    }

    @JRubyMethod
    public IRubyObject servers(ThreadContext context) {
        List<IRubyObject> addresses = new ArrayList<IRubyObject>();
        for (SocketAddress address : client.getAvailableServers()) {
            String addressStr = address.toString();
            if (addressStr.indexOf("/") == 0) {
                addressStr = addressStr.replace("/", "");
            }
            addresses.add(getRuntime().newString(addressStr));
        }
        return getRuntime().newArray(addresses);
    }

    @JRubyMethod(name = "add", required = 2, optional = 3)
    public IRubyObject add(ThreadContext context, IRubyObject[] args) {
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.add(key, timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(getRuntime(), "not stored");
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
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.replace(key, timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(getRuntime(), "not stored");
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
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int timeout = getTimeout(args);
        try {
            boolean result = client.set(key, timeout, value, transcoder).get();
            if (result == false) {
                throw Error.newNotStored(getRuntime(), "not stored");
            }
            return context.nil;
        } catch (ExecutionException ee) {
            throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
        } catch (InterruptedException ie) {
            throw context.runtime.newThreadError(ie.getLocalizedMessage());
        }
    }

    @JRubyMethod(name = "get", required = 1, optional = 1)
    public IRubyObject get(ThreadContext context, IRubyObject[] args) {
        IRubyObject keys = args[0];
        if (keys instanceof RubyString) {
            IRubyObject value = client.get(getFullKey(keys.toString()), transcoder);
            if (value == null) {
                throw Error.newNotFound(getRuntime(), "not found");
            }
            return value;
        } else if (keys instanceof RubyArray) {
            RubyHash results = RubyHash.newHash(getRuntime());

            Map<String, IRubyObject> bulkResults = client.getBulk(getFullKeys(keys.convertToArray()), transcoder);
            for (String key : (List<String>) keys.convertToArray()) {
                if (bulkResults.containsKey(getFullKey(key))) {
                    results.put(key, bulkResults.get(getFullKey(key)));
                }
            }
            return results;
        }
        return context.nil;
    }

    @JRubyMethod(name = "incr", required = 1, optional = 2)
    public IRubyObject incr(ThreadContext context, IRubyObject[] args) {
        String key = getFullKey(args[0].toString());
        int by = getIncrDecrBy(args);
        int timeout = getTimeout(args);
        long result = client.incr(key, by, 1, timeout);
        return getRuntime().newFixnum(result);
    }

    @JRubyMethod(name = "decr", required = 1, optional = 2)
    public IRubyObject decr(ThreadContext context, IRubyObject[] args) {
        String key = getFullKey(args[0].toString());
        int by = getIncrDecrBy(args);
        int timeout = getTimeout(args);
        long result = client.decr(key, by, 0, timeout);
        return getRuntime().newFixnum(result);
    }

    @JRubyMethod
    public IRubyObject delete(ThreadContext context, IRubyObject key) {
        try {
            boolean result = client.delete(getFullKey(key.toString())).get();
            if (result == false) {
                throw Error.newNotFound(getRuntime(), "not found");
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
        RubyHash results = RubyHash.newHash(getRuntime());
        for(Map.Entry<SocketAddress, Map<String, String>> entry : client.getStats().entrySet()) {
            RubyHash serverHash = RubyHash.newHash(getRuntime());
            for(Map.Entry<String, String> server : entry.getValue().entrySet()) {
                serverHash.op_aset(context, getRuntime().newString(server.getKey()), getRuntime().newString(server.getValue()));
            }
            results.op_aset(context, getRuntime().newString(entry.getKey().toString()), serverHash);
        }
        return results;
    }

    @JRubyMethod(name = {"quit", "shutdown"})
    public IRubyObject shutdown(ThreadContext context) {
        client.shutdown();

        return context.nil;
    }

    protected IRubyObject init(ThreadContext context, List<String> servers, RubyHash options) {
        List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
        try {
            ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

            String distributionValue = "ketama";
            String hashValue = "fnv1_32";
            String binaryValue = "false";
            String transcoderValue = null;
            if (!options.isEmpty()) {
                RubyHash opts = options.convertToHash();
                if (opts.containsKey(getRuntime().newSymbol("distribution"))) {
                    distributionValue = opts.get(getRuntime().newSymbol("distribution")).toString();
                }
                if (opts.containsKey(getRuntime().newSymbol("hash"))) {
                    hashValue = opts.get(getRuntime().newSymbol("hash")).toString();
                }
                if (opts.containsKey(getRuntime().newSymbol("binary_protocol"))) {
                    binaryValue = opts.get(getRuntime().newSymbol("binary_protocol")).toString();
                }
                if (opts.containsKey(getRuntime().newSymbol("default_ttl"))) {
                    ttl = Integer.parseInt(opts.get(getRuntime().newSymbol("default_ttl")).toString());
                }
                if (opts.containsKey(getRuntime().newSymbol("namespace"))) {
                    prefixKey = opts.get(getRuntime().newSymbol("namespace")).toString();
                }
                if (opts.containsKey(getRuntime().newSymbol("prefix_key"))) {
                    prefixKey = opts.get(getRuntime().newSymbol("prefix_key")).toString();
                }
                if (opts.containsKey(getRuntime().newSymbol("transcoder"))) {
                    transcoderValue = opts.get(getRuntime().newSymbol("transcoder")).toString();
                }
            }

            if ("array_mod".equals(distributionValue)) {
                builder.setLocatorType(Locator.ARRAY_MOD);
            } else if ("ketama".equals(distributionValue) || "consistent_ketama".equals(distributionValue)) {
                builder.setLocatorType(Locator.CONSISTENT);
            } else {
                throw Error.newNotSupport(getRuntime(), "distribution not support");
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
                throw Error.newNotSupport(getRuntime(), "hash not support");
            }

            if ("true".equals(binaryValue)) {
                builder.setProtocol(Protocol.BINARY);
            }

            client = new MemcachedClient(builder.build(), addresses);

            if ("marshal_zlib".equals(transcoderValue)) {
              transcoder = new MarshalZlibTranscoder(getRuntime());
            } else {
              transcoder = new MarshalTranscoder(getRuntime());
            }
        } catch (IOException ioe) {
            throw context.runtime.newIOErrorFromException(ioe);
        }

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

    private List<String> getFullKeys(List<String> keys) {
        List<String> fullKeys = new ArrayList<String>();
        for (String key : keys) {
            fullKeys.add(getFullKey(key));
        }
        return fullKeys;
    }

    private String getFullKey(String key) {
        return prefixKey + key;
    }
}
