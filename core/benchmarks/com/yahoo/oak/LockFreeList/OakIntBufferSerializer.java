package com.yahoo.oak.LockFreeList;


import com.yahoo.oak.NovaSerializer;
import com.yahoo.oak.UnsafeUtils;

import java.nio.ByteBuffer;


public class OakIntBufferSerializer implements NovaSerializer<ByteBuffer> {

    private final int size;

    public OakIntBufferSerializer(int size) {
        this.size = size;
    }

    @Override
    public void serialize(ByteBuffer obj, long target) {
        copyBuffer(obj, 0, size, target, 0);
    }

    @Override
    public ByteBuffer deserialize(long source, int size) {
        ByteBuffer ret = ByteBuffer.allocate(getSizeBytes());
        copyBuffer(source, 0, size, ret, 0);
        ret.position(0);
        return ret;
    }

    @Override
    public int calculateSize(ByteBuffer buff) {
        return getSizeBytes();
    }

    public int getSizeBytes() {
        return size * Integer.BYTES;
    }

    public static void copyBuffer(ByteBuffer src, int srcPos, int srcSize, long dst, int dstPos) {
        int offset = 0;
        for (int i = 0; i < srcSize; i++) {
            int data = src.getInt(srcPos + offset);
            UnsafeUtils.putInt(dstPos + offset, data);
            offset += Integer.BYTES;
        }
    }
    
    public static void copyBuffer(long src, int srcPos, int srcSize, ByteBuffer dst, int dstPos) {
        int offset = 0;
        for (int i = 0; i < srcSize; i++) {
            int data = UnsafeUtils.getInt(srcPos + offset);       
            dst.putInt(dstPos + offset, data);
            offset += Integer.BYTES;
        }
    }
}
