package com.yahoo.oak.LockFreeList;



import com.yahoo.oak.Facade;
import com.yahoo.oak.NovaSerializer;
import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;

import java.nio.ByteBuffer;

public class MyBuffer implements Comparable<MyBuffer> {

    private static final int DATA_POS = 0;

    public final int capacity;
    public final ByteBuffer buffer;

    public MyBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public int calculateSerializedSize() {
        return capacity;
    }

    @Override // for ConcurrentSkipListMap
    public int compareTo(MyBuffer o) {
        return 0;//OakComparator.compareKeys(this.buffer, o.buffer);
    }
    
    public static int compareBuffers(MyBuffer key1, MyBuffer key2) {
    	return OakIntBufferComparator.compare(key1.buffer, 0, 0, key2.buffer, 0, 0);
    }

    public static void serialize(MyBuffer inputBuffer, long targetAddress) {
        OakIntBufferSerializer.copyBuffer(inputBuffer.buffer, DATA_POS, inputBuffer.capacity / Integer.BYTES,
        		targetAddress, 0);
    }

    public static MyBuffer deserialize(long targetAddress, int size) {
        MyBuffer ret = new MyBuffer(size);
        OakIntBufferSerializer.copyBuffer(targetAddress, 0, size / Integer.BYTES, ret.buffer, DATA_POS);
        return ret;
    }


    public static final NovaSerializer<MyBuffer> DEFAULT_SERIALIZER = new NovaSerializer<MyBuffer>() {

        @Override
        public void serialize(MyBuffer object, long target) {
        	MyBuffer.serialize(object, target);
        }

        @Override
        public MyBuffer deserialize(long target, int size) {
            return MyBuffer.deserialize(target, size);
        }

        @Override
        public int calculateSize(MyBuffer object) {
            return object.calculateSerializedSize();
        }
    };

    public static final OakComparator<MyBuffer> DEFAULT_COMPARATOR = new OakComparator<MyBuffer>() {
        @Override
        public int compareKeys(MyBuffer key1, MyBuffer key2) {
            return MyBuffer.compareBuffers(key1, key2);
        }
    };
}
