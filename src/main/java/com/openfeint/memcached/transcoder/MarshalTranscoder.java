package com.openfeint.memcached.transcoder;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 *
 * MarshalTranscoder does marshaling and unmarshaling.
 *
 */
public class MarshalTranscoder implements Transcoder {
    private Ruby ruby;
    private int flags;
    static final int SPECIAL_LONG = (3 << 8);

    public MarshalTranscoder(Ruby ruby) {
        this(ruby, 0);
    }

    public MarshalTranscoder(Ruby ruby, int flags) {
        this.ruby = ruby;
        this.flags = flags;
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData encode(Object o) {
        if (o instanceof IRubyObject) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                MarshalStream marshal = new MarshalStream(ruby, baos, Integer.MAX_VALUE);
                marshal.dumpObject((IRubyObject) o);
                byte[] bytes = baos.toByteArray();
                return new CachedData(getFlags(), bytes, bytes.length);
            } catch (IOException ioe) {
                throw ruby.newIOErrorFromException(ioe);
            }
        } else {
            byte[] bytes = o.toString().getBytes();
            return new CachedData(SPECIAL_LONG, bytes, bytes.length);
        }
    }

    public Object decode(CachedData d) {
        if (d.getFlags() == SPECIAL_LONG) {
            return Long.parseLong(new String(d.getData()).trim());
        } else {
            try {
                return new UnmarshalStream(ruby, new ByteArrayInputStream(d.getData()), null, false, false).unmarshalObject();
            } catch (IOException ioe) {
                throw ruby.newIOErrorFromException(ioe);
            }
        }
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }

    public int getFlags() {
        return flags;
    }
}
