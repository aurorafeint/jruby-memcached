package com.openfeint.memcached;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyFixnum;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.ArrayList;
import java.util.List;

@JRubyClass(name = "Memcached::Rails", parent = "Memcached")
public class Rails extends Memcached {
    public Rails(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);
    }

    @JRubyMethod(name = "initialize", rest = true)
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        RubyHash opts;
        if (args[args.length - 1] instanceof RubyHash) {
            opts = (RubyHash) args[args.length - 1];
        } else {
            opts = new RubyHash(ruby);
        }
        List<String> servers = new ArrayList<String>();
        for (IRubyObject arg : args) {
            if (arg instanceof RubyString) {
                servers.add(arg.toString());
            } else if (arg instanceof RubyArray) {
                servers.addAll((List<String>) arg.convertToArray());
            }
        }
        if (opts.containsKey(ruby.newSymbol("namespace"))) {
            opts.put(ruby.newSymbol("prefix_key"), opts.get(ruby.newSymbol("namespace")));
        }
        if (opts.containsKey(ruby.newSymbol("namespace_separator"))) {
            opts.put(ruby.newSymbol("prefix_delimiter"), opts.get(ruby.newSymbol("namespace_separator")));
        }
        return super.init(context, servers, opts);
    }

    @JRubyMethod(name = "active?")
    public IRubyObject active_p(ThreadContext context) {
        Ruby ruby = context.getRuntime();
        if (((RubyArray) super.servers(context)).isEmpty()) {
            return ruby.getFalse();
        }
        return ruby.getTrue();
    }

    @JRubyMethod(name = "get", required = 1, optional = 1)
    public IRubyObject get(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        IRubyObject key = args[0];
        RubyBoolean notRaw = notRaw(context, args, 1);
        try {
            return super.get(context, new IRubyObject[] { key, notRaw });
        } catch (RaiseException e) {
            if ("NotFound".equals(e.getException().getMetaClass().getBaseName())) {
                return context.nil;
            }
            throw e;
        }
    }

    @JRubyMethod(name = "read", required = 1, optional = 1)
    public IRubyObject read(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        RubyBoolean not_raw = ruby.getTrue();
        IRubyObject key = args[0];
        RubyBoolean notRaw = notRaw(context, args, 1);
        return get(context, new IRubyObject[] { key, notRaw });
    }

    @JRubyMethod(name = "exist?", required = 1, optional = 1)
    public IRubyObject exist_p(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        try {
            super.get(context, args);
            return ruby.getTrue();
        } catch (RaiseException e) {
            return ruby.getFalse();
        }
    }

    @JRubyMethod(name = "get_multi", required = 1, optional = 1)
    public IRubyObject getMulti(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        IRubyObject keys = args[0];
        RubyBoolean notRaw = notRaw(context, args, 1);
        return super.get(context, new IRubyObject[] { keys, notRaw });
    }

    @JRubyMethod(name = "set", required = 2, optional = 2)
    public IRubyObject set(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        IRubyObject key = args[0];
        IRubyObject value = args[1];
        RubyFixnum ttl = getTTL(context, args, 2);
        RubyBoolean notRaw = notRaw(context, args, 3);
        try {
            super.set(context, new IRubyObject[] { key, value, ttl, notRaw });
            return ruby.getTrue();
        } catch (RaiseException e) {
            return ruby.getFalse();
        }
    }

    @JRubyMethod(name = "write", required = 2, optional = 1)
    public IRubyObject write(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        IRubyObject key = args[0];
        IRubyObject value = args[1];
        RubyFixnum ttl = getTTL(context, args, 2);
        RubyBoolean notRaw = notRaw(context, args, 2);
        return set(context, new IRubyObject[] { key, value, ttl, notRaw });
    }

    @JRubyMethod(name = "fetch", required = 1, optional = 1)
    public IRubyObject fetch(ThreadContext context, IRubyObject[] args, Block block) {
        Ruby ruby = context.getRuntime();
        IRubyObject key = args[0];
        RubyHash options;
        if (args.length > 1) {
            options = (RubyHash) args[1];
        } else {
            options = new RubyHash(ruby);
        }
        IRubyObject value = read(context, args);
        if (value.isNil()) {
            value = block.call(context);
            write(context, new IRubyObject[] { key, value, options });
        }
        return value;
    }

    private RubyFixnum getTTL(ThreadContext context, IRubyObject[] args, int index) {
        Ruby ruby = context.getRuntime();
        if (args.length > index) {
            if (args[index] instanceof RubyFixnum) {
                return (RubyFixnum) args[index];
            } else if (args[index] instanceof RubyHash) {
                RubyHash options = (RubyHash) args[index];
                if (options.containsKey(ruby.newSymbol("ttl"))) {
                    return (RubyFixnum) options.get(ruby.newSymbol("ttl"));
                } else if (options.containsKey(ruby.newSymbol("expires_in"))) {
                    return (RubyFixnum) options.get(ruby.newSymbol("expires_in"));
                }
            }
        }
        return ruby.newFixnum(super.getDefaultTTL());
    }

    private RubyBoolean notRaw(ThreadContext context, IRubyObject[] args, int index) {
        Ruby ruby = context.getRuntime();
        RubyBoolean notRaw = ruby.getTrue();
        if (args.length > index) {
            if (args[index] instanceof RubyBoolean && ruby.getTrue() == (RubyBoolean) args[index]) {
                notRaw = ruby.getFalse();
            } else if (args[index] instanceof RubyHash && ruby.getTrue() == ((RubyHash) args[index]).get(ruby.newSymbol("raw"))) {
                notRaw = ruby.getFalse();
            }
        }
        return notRaw;
    }
}
