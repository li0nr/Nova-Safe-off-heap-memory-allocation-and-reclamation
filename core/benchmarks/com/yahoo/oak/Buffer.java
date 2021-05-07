package com.yahoo.oak;

import java.nio.ByteBuffer;

import com.yahoo.oak.Facade;
import com.yahoo.oak.NovaSerializer;
import com.yahoo.oak.OakComparator;
import com.yahoo.oak.UnsafeUtils;

public class Buffer {
	private final static int DATA_POS = 0;

    public final int capacity;
    public final ByteBuffer buffer;

    public Buffer(int capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public int calculateSerializedSize() {
        return capacity;
    }
    
    public void set(int x ) {
    	buffer.putLong(x);
    	buffer.flip();
    }
    public static final NovaSerializer<Buffer> DEFAULT_SERIALIZER = new NovaSerializer<Buffer>() {
        @Override
        public void serialize(Buffer object, long target) {
            UnsafeUtils.putInt(target , object.capacity);
            int offset = 0;
            for (int i = 0; i < object.capacity/Integer.BYTES; i++) {
                int data = object.buffer.getInt(offset);
                UnsafeUtils.putInt(target + offset + Integer.BYTES, data);
                offset += Integer.BYTES;
            }
        }
        
        @Override
        public Buffer deserialize(long target) {
        	if(target == 0) return null;
        	int offset = 0;
        	int size = UnsafeUtils.getInt(target + offset); 
        	offset +=4;
        	Buffer ret = new Buffer(size);
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target + offset);       
                ret.buffer.putInt(offset - 4, data);
                offset += Integer.BYTES;
            }
            return ret;
        }
        
        
        /*----------------------------------HE SLICE-----------------------------------------------*/

        @Override
        public void serialize(Buffer object, HEslice target) {
            int offset = 0;
            for (int i = 0; i < object.capacity/Integer.BYTES; i++) {
                int data = object.buffer.getInt(offset);
                UnsafeUtils.putInt(target.address + target.offset, data);
                offset += Integer.BYTES;
            }
        }
        
        @Override
        public Buffer deserialize(HEslice target, int size) {
        	if(target == null) return null;
        	Buffer ret = new Buffer(size);
            int offset = 0;
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target.address +target.offset + offset);       
                ret.buffer.putInt(offset, data);
                offset += Integer.BYTES;
            }
            return ret;
        }
        
        /*----------------------------------NOVA SLICE-----------------------------------------------*/
        @Override
        public void serialize(Buffer object, NovaSlice target) {
            int offset = 0;
            for (int i = 0; i < object.capacity/Integer.BYTES; i++) {
                int data = object.buffer.getInt(offset);
                UnsafeUtils.putInt(target.address + offset, data);
                offset += Integer.BYTES;
            }
        }
        
        @Override
        public Buffer deserialize(NovaSlice target, int size) {
        	if(target == null) return null;
        	Buffer ret = new Buffer(size);
            int offset = 0;
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target.address +target.offset + offset);       
                ret.buffer.putInt(offset, data);
                offset += Integer.BYTES;
            }
            return ret;
        }
        
        @Override
        public int calculateSize(Buffer object) {
        	return object.calculateSerializedSize()+Integer.BYTES;
        }
    };
    
    public static final OakComparator<Buffer> DEFAULT_COMPARATOR = new OakComparator<Buffer>() {
    @Override
    public int compareKeys(Buffer key1, Buffer key2) {
            final int minSize = Math.min(key1.capacity, key2.capacity);

            int offset = 0;
            for (int i = 0; i < minSize/Integer.BYTES; i++) {
                int i1 = key1.buffer.getInt(offset);
                int i2 = key2.buffer.getInt(offset);
                int compare = Integer.compare(i1, i2);
                if (compare != 0) {
                    return compare;
                }
                offset += Integer.BYTES;
            }

            return Integer.compare(key1.capacity, key2.capacity);
        }
        
        
        public int compareSerializedKeys(Facade key1, Facade key2, int tid) {
        	Buffer x = (Buffer)key1.Read(DEFAULT_SERIALIZER);
        	Buffer y = (Buffer)key2.Read(DEFAULT_SERIALIZER);

            return compareKeys(x, y);
        }
        
        @Override
        public int compareKeyAndSerializedKey(Buffer key1, Facade key2, int tid) {
        	Buffer y = (Buffer)key2.Read(DEFAULT_SERIALIZER);
        	
            return compareKeys(key1, y);
        }
        
        /*----------------------------------HE SLICE-----------------------------------------------*/

        @Override
        public int compareKeyAndSerializedKey(Buffer key, HEslice serializedKey, int tidx) {
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey,serializedKey.length);

            return compareKeys(key, y);
        	
        }
        
        @Override
        public int compareSerializedKeys(HEslice serializedKey1 , HEslice serializedKey2, int tidx) {
        	Buffer x = DEFAULT_SERIALIZER.deserialize(serializedKey1,serializedKey1.length);
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey2,serializedKey2.length);

            return compareKeys(x, y);
        	
        }
        /*----------------------------------NOVA SLICE-----------------------------------------------*/

        @Override
        public int compareKeyAndSerializedKey(Buffer key, NovaSlice serializedKey, int tidx) {
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey,serializedKey.length);

            return compareKeys(key, y);
        	
        }
        
        @Override
        public int compareSerializedKeys(NovaSlice serializedKey1 , NovaSlice serializedKey2, int tidx) {
        	Buffer x = DEFAULT_SERIALIZER.deserialize(serializedKey1,serializedKey1.length);
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey2,serializedKey2.length);

            return compareKeys(x, y);
        	
        }

    };


}
