package com.openfeint.memcached;

public class ReturnData {
    private int flags;
    private byte[] data;

    public ReturnData(int flags, byte[] data) {
        this.flags = flags;
        this.data = data;
    }


    public int getFlags() {
        return flags;
    }

    public byte[] getData() {
        return data;
    }
}
