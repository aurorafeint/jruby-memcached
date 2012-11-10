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
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@JRubyClass(name = "Memcached")
public class Memcached extends RubyObject {
    private MemcachedClient client;

    private Transcoder transcoder;

    private int ttl;

    private int timeout;

    private int exceptionRetryLimit;

    private String prefixKey;

    public Memcached(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);

        ttl = 604800;
        timeout = -1;
        exceptionRetryLimit = 5;
        prefixKey = "";
    }

    @JRubyMethod(name = "initialize", optional = 2)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        Map<String, String> options = new HashMap<String, String>();
        if (args.length > 1) {
            RubyHash arguments = args[1].convertToHash();
            for (Object key : arguments.keySet()) {
                if (arguments.get(key) != null) {
                    options.put(key.toString(), arguments.get(key).toString());
                }
            }
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
        Ruby ruby = context.getRuntime();
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
        Ruby ruby = context.getRuntime();
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int expiry = getExpiry(args);
        int retry = 0;
        while (true) {
            try {
                Boolean result = (Boolean) client.add(key, expiry, value, transcoder).get();
                if (!result) {
                    throw Error.newNotStored(ruby, "not stored");
                }
                return context.nil;
            } catch (RaiseException e) {
                throw e;
            } catch (ExecutionException e) {
                if ("net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (RuntimeException e) {
                if (e.getCause() != null &&
                    "net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (InterruptedException e) {
                throw ruby.newThreadError(e.getLocalizedMessage());
            }
        }
    }

    @JRubyMethod(name = "replace", required = 2, optional = 3)
    public IRubyObject replace(ThreadContext context, IRubyObject [] args) {
        Ruby ruby = context.getRuntime();
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int expiry = getExpiry(args);
        int retry = 0;
        while (true) {
            try {
                Boolean result = (Boolean) client.replace(key, expiry, value, transcoder).get();
                if (!result) {
                    throw Error.newNotStored(ruby, "not stored");
                }
                return context.nil;
            } catch (RaiseException e) {
                throw e;
            } catch (ExecutionException e) {
                if ("net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (RuntimeException e) {
                if (e.getCause() != null &&
                    "net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (InterruptedException e) {
                throw ruby.newThreadError(e.getLocalizedMessage());
            }
        }
    }

    @JRubyMethod(name = "set", required = 2, optional = 3)
    public IRubyObject set(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        String key = getFullKey(args[0].toString());
        IRubyObject value = args[1];
        int expiry = getExpiry(args);
        int retry = 0;
        while (true) {
            try {
                Boolean result = (Boolean) client.set(key, expiry, value, transcoder).get();
                if (!result) {
                    throw Error.newNotStored(ruby, "not stored");
                }
                return context.nil;
            } catch (RaiseException e) {
                throw e;
            } catch (ExecutionException e) {
                if ("net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (RuntimeException e) {
                if (e.getCause() != null &&
                    "net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (InterruptedException e) {
                throw ruby.newThreadError(e.getLocalizedMessage());
            }
        }
    }

    @JRubyMethod(name = "get", required = 1, optional = 1)
    public IRubyObject get(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        IRubyObject keys = args[0];
        int retry = 0;
        while (true) {
            try {
                if (keys instanceof RubyString) {
                    Object ret = client.get(getFullKey(keys.toString()), transcoder);
                    if (ret == null) {
                        throw Error.newNotFound(ruby, "not found");
                    }
                    IRubyObject value;
                    if (ret instanceof IRubyObject) {
                        value = (IRubyObject) ret;
                    } else {
                        value = ruby.newFixnum((Long) ret);
                    }
                    return value;
                } else if (keys instanceof RubyArray) {
                    RubyHash results = RubyHash.newHash(ruby);

                    Map<String, IRubyObject> bulkResults = (Map<String, IRubyObject>) client.getBulk(getFullKeys(keys.convertToArray()), transcoder);
                    for (String key : (List<String>) keys.convertToArray()) {
                        if (bulkResults.containsKey(getFullKey(key))) {
                            results.put(key, bulkResults.get(getFullKey(key)));
                        }
                    }
                    return results;
                }
            } catch (RaiseException e) {
                throw e;
            } catch (OperationTimeoutException e) {
                if (retry == exceptionRetryLimit) {
                    throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                }
                retry++;
            } catch (RuntimeException e) {
                throw ruby.newRuntimeError(e.getLocalizedMessage());
            }
        }
    }

    @JRubyMethod(name = { "increment", "incr" }, required = 1, optional = 2)
    public IRubyObject incr(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        String key = getFullKey(args[0].toString());
        int by = getIncrDecrBy(args);
        int expiry = getExpiry(args);
        int retry = 0;
        while (true) {
            try {
                long result = client.incr(key, by, 1, expiry);
                return ruby.newFixnum(result);
            } catch (OperationTimeoutException e) {
                if (retry == exceptionRetryLimit) {
                    throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                }
                retry++;
            }
        }
    }

    @JRubyMethod(name = { "decrement", "decr" }, required = 1, optional = 2)
    public IRubyObject decr(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        String key = getFullKey(args[0].toString());
        int by = getIncrDecrBy(args);
        int expiry = getExpiry(args);
        int retry = 0;
        while (true) {
            try {
                long result = client.decr(key, by, 0, expiry);
                return ruby.newFixnum(result);
            } catch (OperationTimeoutException e) {
                if (retry == exceptionRetryLimit) {
                    throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                }
                retry++;
            }
        }
    }

    @JRubyMethod(name = "delete")
    public IRubyObject delete(ThreadContext context, IRubyObject key) {
        Ruby ruby = context.getRuntime();
        int retry = 0;
        while (true) {
            try {
                boolean result = client.delete(getFullKey(key.toString())).get();
                if (!result) {
                    throw Error.newNotFound(ruby, "not found");
                }
                return context.nil;
            } catch (RaiseException e) {
                throw e;
            } catch (ExecutionException e) {
                if ("net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (RuntimeException e) {
                if (e.getCause() != null &&
                    "net.spy.memcached.internal.CheckedOperationTimeoutException".equals(e.getCause().getClass().getName())) {
                    if (retry == exceptionRetryLimit) {
                        throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
                    }
                    retry++;
                } else {
                    throw ruby.newRuntimeError(e.getLocalizedMessage());
                }
            } catch (InterruptedException e) {
                throw ruby.newThreadError(e.getLocalizedMessage());
            }
        }
    }

    @JRubyMethod
    public IRubyObject flush(ThreadContext context) {
        Ruby ruby = context.getRuntime();
        try {
            client.flush().get();
            return context.nil;
        } catch (OperationTimeoutException e) {
            throw Error.newATimeoutOccurred(ruby, e.getLocalizedMessage());
        } catch (ExecutionException e) {
            throw ruby.newRuntimeError(e.getLocalizedMessage());
        } catch (InterruptedException e) {
            throw ruby.newThreadError(e.getLocalizedMessage());
        }
    }

    @JRubyMethod
    public IRubyObject stats(ThreadContext context) {
        Ruby ruby = context.getRuntime();
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

    @JRubyMethod(name = {"quit", "shutdown"})
    public IRubyObject shutdown(ThreadContext context) {
        client.shutdown();

        return context.nil;
    }

    protected int getDefaultTTL() {
        return ttl;
    }

    protected IRubyObject init(ThreadContext context, List<String> servers, Map<String, String> opts) {
        Ruby ruby = context.getRuntime();
        List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
        try {
            ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

            String distributionValue = "ketama";
            String hashValue = "fnv1_32";
            boolean binaryValue = false;
            boolean shouldOptimize = false;
            String transcoderValue = null;
            if (!opts.isEmpty()) {
                if (opts.containsKey("distribution")) {
                    distributionValue = opts.get("distribution");
                }
                if (opts.containsKey("hash")) {
                    hashValue = opts.get("hash");
                }
                if (opts.containsKey("binary_protocol")) {
                    binaryValue = Boolean.parseBoolean(opts.get("binary_protocol"));
                }
                if (opts.containsKey("should_optimize")) {
                    shouldOptimize = Boolean.parseBoolean(opts.get("should_optimize"));
                }
                if (opts.containsKey("default_ttl")) {
                    ttl = Integer.parseInt(opts.get("default_ttl"));
                }
                if (opts.containsKey("timeout")) {
                    timeout = Integer.parseInt(opts.get("timeout"));
                }
                if (opts.containsKey("exception_retry_limit")) {
                    exceptionRetryLimit = Integer.parseInt(opts.get("exception_retry_limit"));
                }
                if (opts.containsKey("namespace")) {
                    prefixKey = opts.get("namespace");
                }
                if (opts.containsKey("prefix_key")) {
                    prefixKey = opts.get("prefix_key");
                }
                if (opts.containsKey("transcoder")) {
                    transcoderValue = opts.get("transcoder");
                }
            }

            if ("array_mod".equals(distributionValue)) {
                builder.setLocatorType(Locator.ARRAY_MOD);
            } else if ("ketama".equals(distributionValue) || "consistent_ketama".equals(distributionValue)) {
                builder.setLocatorType(Locator.CONSISTENT);
            } else {
                throw Error.newNotSupport(ruby, "distribution not support");
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

            if (binaryValue) {
                builder.setProtocol(Protocol.BINARY);
            }
            if (shouldOptimize) {
                builder.setShouldOptimize(true);
            }

            if (timeout != -1) {
                builder.setOpTimeout(timeout);
            }
            builder.setDaemon(true);
            if ("marshal_zlib".equals(transcoderValue)) {
                transcoder = new MarshalZlibTranscoder(ruby);
            } else {
                transcoder = new MarshalTranscoder(ruby);
            }
            builder.setTranscoder(transcoder);

            client = new MemcachedClient(builder.build(), addresses);

            return context.nil;
        } catch (IOException e) {
            throw ruby.newIOErrorFromException(e);
        }
    }

    private int getExpiry(IRubyObject[] args) {
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
