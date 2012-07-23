package com.openfeint.memcached.transcoders;

import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.CompressionMode;
import net.rubyeye.xmemcached.transcoders.Transcoder;

import com.openfeint.memcached.ReturnData;

/**
 *
 * SimpleTranscoder didn't do any serializing/deserializing or compression/decompression.
 * Ruby will convert object to string by Marshal and finally passing bytes[].
 *
 */
public class SimpleTranscoder implements Transcoder<Object> {
    private int flags;

    public CachedData encode(Object o) {
        byte[] b = (byte[]) o;
        return new CachedData(getFlags(), b);
    }

    public Object decode(CachedData d) {
        byte[] data = d.getData();
        int flags = d.getFlag();
        return new ReturnData(flags, data);
    }

    public void setPrimitiveAsString(boolean primitiveAsString) { }
    public void setPackZeros(boolean packZeros) { }
    public void setCompressionThreshold(int to) { }
    public void setCompressionMode(CompressionMode compressMode) { }
    public boolean isPrimitiveAsString() { return false; }
    public boolean isPackZeros() { return false; }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }
}
