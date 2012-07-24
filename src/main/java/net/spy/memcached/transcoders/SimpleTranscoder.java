package net.spy.memcached.transcoders;

import net.spy.memcached.CachedData;
import net.spy.memcached.ReturnData;
import net.spy.memcached.transcoders.Transcoder;

/**
 *
 * SimpleTranscoder didn't do any serializing/deserializing or compression/decompression.
 * Ruby will convert object to string by Marshal and finally passing bytes[].
 *
 */
public class SimpleTranscoder implements Transcoder<Object> {
    private final int maxSize;
    private int flags;

    public SimpleTranscoder() {
        this(CachedData.MAX_SIZE, 0);
    }

    public SimpleTranscoder(int flags) {
        this(CachedData.MAX_SIZE, flags);
    }

    public SimpleTranscoder(int maxSize, int flags) {
        this.maxSize = maxSize;
        this.flags = flags;
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData encode(Object o) {
        byte[] b = (byte[]) o;
        return new CachedData(getFlags(), b, getMaxSize());
    }

    public Object decode(CachedData d) {
        byte[] data = d.getData();
        int flags = d.getFlags();
        return new ReturnData(flags, data);
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }
}
