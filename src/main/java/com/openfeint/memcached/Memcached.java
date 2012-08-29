package com.openfeint.memcached;

import com.openfeint.memcached.error.Error;
import net.spy.memcached.OperationTimeoutException;
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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@JRubyClass(name = "Memcached")
public class Memcached extends RubyObject {
    private ConnectionPool pool;

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
                options.put(key.toString(), arguments.get(key).toString());
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
        init(context, servers, options);

        return context.nil;
    }

    @JRubyMethod
    public IRubyObject servers(ThreadContext context) {
        Ruby ruby = context.getRuntime();
        List<IRubyObject> addresses = new ArrayList<IRubyObject>();
        for (SocketAddress address : pool.getConnection().getAvailableServers()) {
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
                Boolean result = (Boolean) pool.getConnection().add(key, expiry, value).get();
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
                Boolean result = (Boolean) pool.getConnection().replace(key, expiry, value).get();
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
                Boolean result = (Boolean) pool.getConnection().set(key, expiry, value).get();
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
                    Object ret = pool.getConnection().get(getFullKey(keys.toString()));
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

                    Map<String, Object> bulkResults = (Map<String, Object>) pool.getConnection().getBulk(getFullKeys(keys.convertToArray()));
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
                long result = pool.getConnection().incr(key, by, 1, expiry);
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
                long result = pool.getConnection().decr(key, by, 0, expiry);
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
                boolean result = pool.getConnection().delete(getFullKey(key.toString())).get();
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
            pool.getConnection().flush().get();
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
        for(Map.Entry<SocketAddress, Map<String, String>> entry : pool.getConnection().getStats().entrySet()) {
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
        pool.shutdown();

        return context.nil;
    }

    protected int getDefaultTTL() {
        return ttl;
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

    protected void init(ThreadContext context, List<String> servers, Map<String, String> options) {
        if (options.containsKey("default_ttl")) {
            ttl = Integer.parseInt(options.get("default_ttl"));
        }

        if (options.containsKey("exception_retry_limit")) {
            exceptionRetryLimit = Integer.parseInt(options.get("exception_retry_limit"));
        }

        if (options.containsKey("timeout")) {
            timeout = Integer.parseInt(options.get("timeout"));
        }

        if (options.containsKey("prefix_key")) {
            prefixKey = options.get("prefix_key");
        }
        if (options.containsKey("namespace")) {
            prefixKey = options.get("namespace");
        }

        int poolSize = 1;
        if (options.containsKey("pool_size")) {
            poolSize = Integer.parseInt(options.get("pool_size"));
        }
        pool = new ConnectionPool(context, servers, options, poolSize);
    }
}
