package com.openfeint.memcached.transcoder;

import com.jcraft.jzlib.ZOutputStream;
import com.jcraft.jzlib.ZInputStream;
import com.jcraft.jzlib.JZlib;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;
import org.jruby.util.ByteList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;

/**
 *
 * MarshalZlibTranscoder do marshaling/unmarshaling and compressing/decompressing with zlib.
 *
 */
public class MarshalZlibTranscoder extends MarshalTranscoder {
    static final int COMPRESS_FLAG = 1;

    public MarshalZlibTranscoder(Ruby ruby) {
        super(ruby, COMPRESS_FLAG);
    }

    public MarshalZlibTranscoder(Ruby ruby, int flags) {
        super(ruby, flags);
    }

    public CachedData encode(Object o) {
        if (o instanceof IRubyObject) {
            try {
                ByteArrayOutputStream out1 = new ByteArrayOutputStream();
                MarshalStream marshal = new MarshalStream(ruby, out1, Integer.MAX_VALUE);
                marshal.dumpObject((IRubyObject) o);

                byte[] bytes;
                if (getFlags() == COMPRESS_FLAG) {
                    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
                    ZOutputStream zout = new ZOutputStream(out2, JZlib.Z_DEFAULT_COMPRESSION);
                    zout.write(out1.toByteArray());
                    zout.close();
                    bytes = out2.toByteArray();
                } else {
                    bytes = out1.toByteArray();
                }

                return new CachedData(super.getFlags(), bytes, bytes.length);
            } catch (IOException e) {
                throw ruby.newIOErrorFromException(e);
            }
        } else {
            return super.encodeNumber(o);
        }
    }

    public Object decode(CachedData d) {
        try {
            byte[] bytes;
            if (d.getFlags() == COMPRESS_FLAG) {
                ByteArrayInputStream in = new ByteArrayInputStream(d.getData());
                ZInputStream zin = new ZInputStream(in);

                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int nRead;
                byte[] data = new byte[1024];
                while ((nRead = zin.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                buffer.flush();
                bytes = buffer.toByteArray();
                zin.close();
            } else {
                bytes = d.getData();
            }

            return new UnmarshalStream(ruby, new ByteArrayInputStream(bytes), null, false, false).unmarshalObject();
        } catch (RaiseException e) {
            return super.decodeNumber(d, e);
        } catch (IOException e) {
            return super.decodeNumber(d, e);
        }
    }
}
