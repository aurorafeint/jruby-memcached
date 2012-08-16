package com.openfeint.memcached;

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
        RubyBoolean notRaw = notRaw(context, args);
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
        RubyBoolean notRaw = notRaw(context, args);
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
        RubyBoolean notRaw = notRaw(context, args);
        return super.get(context, new IRubyObject[] { keys, notRaw });
    }

    private RubyBoolean notRaw(ThreadContext context, IRubyObject[] args) {
        Ruby ruby = context.getRuntime();
        RubyBoolean notRaw = ruby.getTrue();
        if (args.length > 1) {
            if (args[1] instanceof RubyBoolean && ruby.getTrue() == (RubyBoolean) args[1]) {
                notRaw = ruby.getFalse();
            }
            if (args[1] instanceof RubyHash && ruby.getTrue() == ((RubyHash) args[1]).get(ruby.newSymbol("raw"))) {
                notRaw = ruby.getFalse();
            }
        }
        return notRaw;
    }
}
