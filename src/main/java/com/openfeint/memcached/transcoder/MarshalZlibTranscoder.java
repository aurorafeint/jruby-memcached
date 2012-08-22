package com.openfeint.memcached.transcoder;

import com.jcraft.jzlib.ZOutputStream;
import com.jcraft.jzlib.ZInputStream;
import com.jcraft.jzlib.JZlib;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
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
public class MarshalZlibTranscoder implements Transcoder {
    private Ruby ruby;
    private int flags;

    public MarshalZlibTranscoder(Ruby ruby) {
        this(ruby, 1);
    }

    public MarshalZlibTranscoder(Ruby ruby, int flags) {
        this.ruby = ruby;
        this.flags = flags;
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData encode(Object o) {
        if (o instanceof IRubyObject) {
            try {
                ByteArrayOutputStream out1 = new ByteArrayOutputStream();
                MarshalStream marshal = new MarshalStream(ruby, out1, Integer.MAX_VALUE);
                marshal.dumpObject((IRubyObject) o);

                byte[] bytes;
                if (flags == 1) {
                    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
                    ZOutputStream zout = new ZOutputStream(out2, JZlib.Z_DEFAULT_COMPRESSION);
                    zout.write(out1.toByteArray());
                    zout.close();
                    bytes = out2.toByteArray();
                } else {
                    bytes = out1.toByteArray();
                }

                return new CachedData(getFlags(), bytes, bytes.length);
            } catch (IOException ioe) {
                throw ruby.newIOErrorFromException(ioe);
            }
        } else {
            byte[] bytes = o.toString().getBytes();
            return new CachedData(MarshalTranscoder.SPECIAL_LONG, bytes, bytes.length);
        }
    }

    public Object decode(CachedData d) {
        if (d.getFlags() == MarshalTranscoder.SPECIAL_LONG) {
            return Long.parseLong(new String(d.getData()));
        } else {
            try {
                byte[] bytes;
                if (d.getFlags() == 1) {
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
