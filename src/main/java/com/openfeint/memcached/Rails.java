package com.openfeint.memcached;

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

import java.util.ArrayList;
import java.util.List;

@JRubyClass(name = "Memcached::Rails", parent = "Memcached")
class Rails extends Memcached {
    public Rails(final Ruby ruby, RubyClass rubyClass) {
        super(ruby, rubyClass);
    }

    @JRubyMethod(name = "initialize")
    public IRubyObject initialize(ThreadContext context, IRubyObject[] args) {
        RubyHash opts;
        if (args[args.length - 1] instanceof RubyHash) {
            opts = (RubyHash) args[args.length - 1];
        } else {
            opts = new RubyHash(getRuntime());
        }
        List<String> servers = new ArrayList<String>();
        for (IRubyObject arg : args) {
            if (arg instanceof RubyString) {
                servers.add(arg.toString());
            } else if (arg instanceof RubyArray) {
                servers.addAll((List<String>) arg.convertToArray());
            }
        }
        if (opts.containsKey(getRuntime().newSymbol("namespace"))) {
            opts.put(getRuntime().newSymbol("prefix_key"), opts.get(getRuntime().newSymbol("namespace")));
        }
        if (opts.containsKey(getRuntime().newSymbol("namespace_separator"))) {
            opts.put(getRuntime().newSymbol("prefix_delimiter"), opts.get(getRuntime().newSymbol("namespace_separator")));
        }
        return init(context, servers, opts);
    }
}
