package com.yahoo.oak.LockFreeList;

import com.yahoo.oak.OakComparator;
import com.yahoo.oak.UnsafeUtils;

import java.nio.ByteBuffer;

public class OakIntBufferComparator implements OakComparator<ByteBuffer> {

    private final int size;

    public OakIntBufferComparator(int size) {
        this.size = size;
    }

    @Override
    public int compareKeys(ByteBuffer buff1, ByteBuffer buff2) {
        return compare(buff1, 0, size, buff2, 0, size);
    }

    public static int compare(ByteBuffer buff1, int pos1, int size1, 
        long address, int pos2, int size2) {
        
        int minSize = Math.min(size1, size2);

        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + Integer.BYTES * i);
            int i2 = UnsafeUtils.getInt(pos2 + Integer.BYTES * i);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
        }

        return Integer.compare(size1, size2);
    }
    
    public static int compare(long address1, int pos1, int size1, 
            long address, int pos2, int size2) {
            
            int minSize = Math.min(size1, size2);

            for (int i = 0; i < minSize; i++) {
                int i1 = UnsafeUtils.getInt(address1+ pos1 + Integer.BYTES * i);
                int i2 = UnsafeUtils.getInt(address + pos2 + Integer.BYTES * i);
                int compare = Integer.compare(i1, i2);
                if (compare != 0) {
                    return compare;
                }
            }

            return Integer.compare(size1, size2);
        }
    
    
    public static int compare(ByteBuffer buff1, int pos1, int size1, ByteBuffer buff2, int pos2, int size2) {
        final int minSize = Math.min(size1, size2);

        int offset = 0;
        for (int i = 0; i < minSize; i++) {
            int i1 = buff1.getInt(pos1 + offset);
            int i2 = buff2.getInt(pos2 + offset);
            int compare = Integer.compare(i1, i2);
            if (compare != 0) {
                return compare;
            }
            offset += Integer.BYTES;
        }

        return Integer.compare(size1, size2);
    }
}
