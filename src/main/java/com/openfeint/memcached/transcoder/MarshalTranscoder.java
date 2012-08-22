package com.openfeint.memcached.transcoder;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.exceptions.RaiseException;
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
    protected Ruby ruby;
    private int flags;

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
            } catch (IOException e) {
                throw ruby.newIOErrorFromException(e);
            }
        } else {
            return encodeNumber(o);
        }
    }

    public Object decode(CachedData d) {
        try {
            return new UnmarshalStream(ruby, new ByteArrayInputStream(d.getData()), null, false, false).unmarshalObject();
        } catch (RaiseException e) {
            return decodeNumber(d, e);
        } catch (IOException e) {
            return decodeNumber(d, e);
        }
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }

    public int getFlags() {
        return flags;
    }

    protected CachedData encodeNumber(Object o) {
        byte[] bytes = o.toString().getBytes();
        return new CachedData(getFlags(), bytes, bytes.length);
    }

    protected Long decodeNumber(CachedData d, RaiseException originalException) {
        try {
            return Long.valueOf(new String(d.getData()).trim());
        } catch (NumberFormatException e) {
            throw ruby.newRuntimeError(originalException.getLocalizedMessage());
        }
    }

    protected Long decodeNumber(CachedData d, IOException originalException) {
        try {
            return Long.valueOf(new String(d.getData()).trim());
        } catch (NumberFormatException e) {
            throw ruby.newIOErrorFromException(originalException);
        }
    }
}
