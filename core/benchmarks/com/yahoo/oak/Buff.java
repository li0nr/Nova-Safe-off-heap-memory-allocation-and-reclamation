package com.yahoo.oak;

import java.nio.ByteBuffer;

public class Buff {
	public final int capacity;
	public final ByteBuffer buffer;

	public Buff(int capacity) {
		this.capacity = capacity;
		this.buffer = ByteBuffer.allocate(capacity);
	}

	public Buff() {
		this.capacity = 4;
		this.buffer = ByteBuffer.allocate(capacity);
	}

	public int calculateSerializedSize() {
		return capacity;
	}

	public int get() {
		return buffer.getInt(0);
	}
	
	public void set(int x) {
		int i = 0;
		while(i <capacity/Integer.BYTES) {
			i++;
			buffer.putInt(i,x);
		}
		this.DebugPrint();

	}
	
	
	public void DebugPrint() {
		int i = 0;
		while(i <capacity/Integer.BYTES) {
			i++;
			buffer.getInt(i);
			System.out.print(buffer.getInt(i)+ "**");
		}
	}
	
	public int compare(Buff o) {
		final int minSize = Math.min(this.capacity, o.capacity);
		int offset = 0;
		for (int i = 0; i < minSize / Integer.BYTES; i++) { 
			int i1 = this.buffer.getInt(offset); 
			int i2 = o.buffer.getInt(offset);
			int compare= Integer.compare(i1, i2); 
			if (compare != 0) 
				return compare;  
			offset +=	  Integer.BYTES;
			}
		return Integer.compare(this.capacity, o.capacity);
		
	}

	public static final NovaSerializer<Buff> DEFAULT_SERIALIZER = new NovaSerializer<Buff>() {
		@Override
		public void serialize(Buff object, long output) {
			long targetPos = output;
			UnsafeUtils.putInt(targetPos, object.capacity);
			targetPos += Integer.BYTES;
			object.DebugPrint();
			int offset = 0;
			for (int i = 0; i < object.capacity / Integer.BYTES; i++) {
				int data = object.buffer.getInt(offset);
				UnsafeUtils.putInt(targetPos + offset, data);
				offset += Integer.BYTES;
			}
		}

		@Override
		public Buff deserialize(long input) {
			long inputPos = input;
			int capacity = UnsafeUtils.getInt(inputPos);
			inputPos += Integer.BYTES;

			Buff ret = new Buff(capacity);

			int offset = 0;
			for (int i = 0; i < capacity / Integer.BYTES; i++) {
				int data = UnsafeUtils.getInt(inputPos + offset);
				ret.buffer.putInt(offset, data);
				offset += Integer.BYTES;
			}
			return ret;
		}


		@Override
		public int calculateSize(Buff object) {
			return object.calculateSerializedSize() + Integer.BYTES;
		}
	};

	public static final NovaC<Buff> DEFAULT_C = new NovaC<Buff>() {

		@Override
		public int compareKeys(Buff key1, Buff key2) {
			return 0;
		}

		@Override
		public int compareKeys(long address, Buff obj) {
			int offset = 0;

			int size = UnsafeUtils.getInt(address);
			final int minSize = Math.min(size, obj.capacity);

			address = address + Integer.BYTES;
			for (int i = 0; i < minSize / Integer.BYTES; i++) {
				int i1 = UnsafeUtils.unsafe.getInt(address + offset);
				int i2 = obj.buffer.getInt(offset);
				int compare = Integer.compare(i1, i2);
				if (compare != 0) {
					return compare;
				}
				offset += Integer.BYTES;
			}
			return Integer.compare(size, obj.capacity);
		}
	};

	/*
	 * public static final NovaComparator<Buff> DEFAULT_COMPARATOR = new
	 * NovaComparator<Buff>() {
	 * 
	 * @Override public int compareKeys(Buff key1, Buff key2) { final int minSize =
	 * Math.min(key1.capacity, key2.capacity);
	 * 
	 * int offset = 0; for (int i = 0; i < minSize / Integer.BYTES; i++) { int i1 =
	 * key1.buffer.getInt(offset); int i2 = key2.buffer.getInt(offset); int compare
	 * = Integer.compare(i1, i2); if (compare != 0) { return compare; } offset +=
	 * Integer.BYTES; }
	 * 
	 * return Integer.compare(key1.capacity, key2.capacity); }
	 * 
	 * public int compareSerializedKeys(Facade<Buff> key1, Facade<Buff> key2, int
	 * tid) { Buff x = (Buff) key1.Read(DEFAULT_SERIALIZER); Buff y = (Buff)
	 * key2.Read(DEFAULT_SERIALIZER);
	 * 
	 * return compareKeys(x, y); }
	 * 
	 * @Override public int compareKeyAndSerializedKey(Buff key1, Facade<Buff> key2,
	 * int tid) { Buff y = (Buff) key2.Read(DEFAULT_SERIALIZER);
	 * 
	 * return compareKeys(key1, y); }
	 * 
	 * ----------------------------------HE
	 * SLICE-----------------------------------------------
	 * 
	 * @Override public int compareKeyAndSerializedKey(Buff key, HEslice
	 * serializedKey, int tidx) { final int minSize = Math.min(key.capacity,
	 * serializedKey.length);
	 * 
	 * int offset = 0; for (int i = 0; i < minSize / Integer.BYTES; i++) { int i1 =
	 * key.buffer.getInt(offset); int i2 =
	 * UnsafeUtils.unsafe.getInt(serializedKey.address + serializedKey.offset +
	 * Integer.BYTES); int compare = Integer.compare(i1, i2); if (compare != 0) {
	 * return compare; } offset += Integer.BYTES; } return
	 * Integer.compare(key.capacity, serializedKey.length);
	 * 
	 * }
	 * 
	 * };
	 */
	
	
	
	/*
	 * ----------------------------------HE
	 * SLICE-----------------------------------------------
	 * 
	 * 
	 * @Override public void serialize(Buff object, HEslice target) {
	 * UnsafeUtils.putInt(target.address +target.offset, object.capacity); int
	 * offset = 0; for (int i = 0; i < object.capacity/Integer.BYTES; i++) { int
	 * data = object.buffer.getInt(offset); UnsafeUtils.putInt(target.address +
	 * target.offset +offset + Integer.BYTES, data); offset += Integer.BYTES; } }
	 * 
	 * @Override public Buff deserialize(HEslice target) { if(target == null) return
	 * null; int offset = 0; int size = UnsafeUtils.getInt(target.address); offset
	 * += Integer.BYTES; Buff ret = new Buff(size); for (int i = 0; i <
	 * size/Integer.BYTES; i++) { int data = UnsafeUtils.getInt(target.address
	 * +target.offset + offset); ret.buffer.putInt(offset - Integer.BYTES, data);
	 * offset += Integer.BYTES; } return ret; }
	 * 
	 * 
	 * ----------------------------------NOVA
	 * SLICE-----------------------------------------------
	 * 
	 * @Override public void serialize(Buff object, NovaSlice target) { int offset =
	 * 0; for (int i = 0; i < object.capacity/Integer.BYTES; i++) { int data =
	 * object.buffer.getInt(offset); UnsafeUtils.putInt(target.address + offset,
	 * data); offset += Integer.BYTES; } }
	 * 
	 * @Override public Buff deserialize(NovaSlice target) { if(target == null)
	 * return null; int offset = 0; int size = UnsafeUtils.getInt(target.address);
	 * offset += Integer.BYTES; Buff ret = new Buff(size); for (int i = 0; i <
	 * size/Integer.BYTES; i++) { int data = UnsafeUtils.getInt(target.address
	 * +target.offset + offset); ret.buffer.putInt(offset - Integer.BYTES, data);
	 * offset += Integer.BYTES; } return ret; }
	 */

}
