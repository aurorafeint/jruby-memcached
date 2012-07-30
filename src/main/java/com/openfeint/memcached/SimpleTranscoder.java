package com.openfeint.memcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

/**
 *
 * SimpleTranscoder do marshaling and unmarshaling.
 *
 */
public class SimpleTranscoder implements Transcoder<IRubyObject> {
    private Ruby ruby;
    private int flags;

    public SimpleTranscoder(Ruby ruby) {
        this(ruby, 0);
    }

    public SimpleTranscoder(Ruby ruby, int flags) {
        this.ruby = ruby;
        this.flags = flags;
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData encode(IRubyObject o) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MarshalStream marshal = new MarshalStream(ruby, baos, Integer.MAX_VALUE);
            marshal.dumpObject(o);
            byte[] bytes = baos.toByteArray();
            return new CachedData(getFlags(), bytes, bytes.length);
        } catch (IOException ioe) {
            throw ruby.newIOErrorFromException(ioe);
        }
    }

    public IRubyObject decode(CachedData d) {
        try {
            return new UnmarshalStream(ruby, new ByteArrayInputStream(d.getData()), null, false, false).unmarshalObject();
        } catch (IOException ioe) {
            throw ruby.newIOErrorFromException(ioe);
        }
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }

    public int getFlags() {
        return flags;
    }
}
