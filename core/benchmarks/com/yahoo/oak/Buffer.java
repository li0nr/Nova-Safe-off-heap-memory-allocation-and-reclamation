package com.yahoo.oak;

import java.nio.ByteBuffer;

public class Buffer {
    public final int capacity;
    public final ByteBuffer buffer;

    public Buffer(int capacity) {
        this.capacity = capacity;
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public Buffer() {
        this.capacity = 8;
        this.buffer = ByteBuffer.allocate(capacity + Integer.BYTES);
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
        	int size = UnsafeUtils.getInt(target); 
        	offset += Integer.BYTES;
        	Buffer ret = new Buffer(size);
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target + offset);       
                ret.buffer.putInt(offset - Integer.BYTES, data);
                offset += Integer.BYTES;
            }
            return ret;
        }
        
        
        /*----------------------------------HE SLICE-----------------------------------------------*/

        @Override
        public void serialize(Buffer object, HEslice target) {
            UnsafeUtils.putInt(target.address, object.capacity);
            int offset = 0;
            for (int i = 0; i < object.capacity/Integer.BYTES; i++) {
                int data = object.buffer.getInt(offset);
                UnsafeUtils.putInt(target.address + target.offset +offset + Integer.BYTES, data);
                offset += Integer.BYTES;
            }
        }
        
        @Override
        public Buffer deserialize(HEslice target) {
        	if(target == null) return null;
        	int offset = 0;
        	int size = UnsafeUtils.getInt(target.address); 
        	offset += Integer.BYTES;        	
        	Buffer ret = new Buffer(size);
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target.address +target.offset + offset);       
                ret.buffer.putInt(offset - Integer.BYTES, data);
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
        public Buffer deserialize(NovaSlice target) {
        	if(target == null) return null;
        	int offset = 0;
        	int size = UnsafeUtils.getInt(target.address); 
        	offset += Integer.BYTES;        	
        	Buffer ret = new Buffer(size);
            for (int i = 0; i < size/Integer.BYTES; i++) {
                int data = UnsafeUtils.getInt(target.address +target.offset + offset);       
                ret.buffer.putInt(offset - Integer.BYTES, data);
                offset += Integer.BYTES;
            }
            return ret;
        }
        
        @Override
        public int calculateSize(Buffer object) {
        	return object.calculateSerializedSize()+Integer.BYTES;
        }
    };
    
    public static final NovaComparator<Buffer> DEFAULT_COMPARATOR = new NovaComparator<Buffer>() {
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
        
        
        public int compareSerializedKeys(Facade<Buffer> key1, Facade<Buffer> key2, int tid) {
        	Buffer x = (Buffer)key1.Read(DEFAULT_SERIALIZER);
        	Buffer y = (Buffer)key2.Read(DEFAULT_SERIALIZER);

            return compareKeys(x, y);
        }
        
        @Override
        public int compareKeyAndSerializedKey(Buffer  key1,  Facade<Buffer> key2, int tid) {
        	Buffer y = (Buffer)key2.Read(DEFAULT_SERIALIZER);
        	
            return compareKeys(key1, y);
        }
        
        /*----------------------------------HE SLICE-----------------------------------------------*/

        @Override
        public int compareKeyAndSerializedKey(Buffer key, HEslice serializedKey, int tidx) {
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey);

            return compareKeys(key, y);
        	
        }
        
        @Override
        public int compareSerializedKeys(HEslice serializedKey1 , HEslice serializedKey2, int tidx) {
        	Buffer x = DEFAULT_SERIALIZER.deserialize(serializedKey1);
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey2);

            return compareKeys(x, y);
        	
        }
        /*----------------------------------NOVA SLICE-----------------------------------------------*/

        @Override
        public int compareKeyAndSerializedKey(Buffer key, NovaSlice serializedKey, int tidx) {
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey);

            return compareKeys(key, y);
        	
        }
        
        @Override
        public int compareSerializedKeys(NovaSlice serializedKey1 , NovaSlice serializedKey2, int tidx) {
        	Buffer x = DEFAULT_SERIALIZER.deserialize(serializedKey1);
        	Buffer y = DEFAULT_SERIALIZER.deserialize(serializedKey2);

            return compareKeys(x, y);
        	
        }

    };


}
