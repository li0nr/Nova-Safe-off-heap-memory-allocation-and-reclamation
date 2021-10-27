package com.yahoo.oak.Buff;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.yahoo.oak.CopyConstructor;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;

public class Buff  implements Comparable<Buff>{
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
			buffer.putInt(i*Integer.BYTES,x+i);
			i++;
		}
		//this.DebugPrint();
	}
	
	
	public void DebugPrint() {
		int i = 0;
		while(i <capacity/Integer.BYTES) {
			buffer.getInt(i);
			System.out.print(buffer.getInt(i)+ "**");
			i++;
		}
	}
	
	@Override
	public int compareTo(Buff o) {
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
	

	
    public boolean equals(Buff o) {
		final int minSize = Math.min(this.capacity, o.capacity);
		int offset = 0;
		for (int i = 0; i < minSize / Integer.BYTES; i++) { 
			int i1 = this.buffer.getInt(offset); 
			int i2 = o.buffer.getInt(offset);
			int compare= Integer.compare(i1, i2); 
			if (compare != 0) 
				return false;  
			offset +=	  Integer.BYTES;
			}
		return Integer.compare(this.capacity, o.capacity) == 0 ? true :false ;
    }

	public static final NovaS<Buff> DEFAULT_SERIALIZER = new NovaS<Buff>() {
		@Override
		public void serialize(Buff object, long output) {
			long targetPos = output;
			UnsafeUtils.putInt(targetPos, object.capacity);
			targetPos += Integer.BYTES;
			int offset = 0;
			for (int i = 0; i < object.capacity / Integer.BYTES; i++) {
				int data = object.buffer.getInt(offset);
				UnsafeUtils.putInt(targetPos + offset, data);
				offset += Integer.BYTES;
			}
		}
		
		@Override
		public void serialize(long source, long output) {
			long targetPos = output;
			int capacity = UnsafeUtils.getInt( source);
			UnsafeUtils.putInt(targetPos, capacity);
			int offset = 4;
			for (int i = 0; i < capacity / Integer.BYTES; i++) {
				UnsafeUtils.putInt(targetPos + offset, UnsafeUtils.getInt( source +offset));
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
			throw new IllegalAccessError();
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
		
		public int compareKeys(long address, long address2) {
			int offset = 0;

			int size1 = UnsafeUtils.getInt(address);
			int size2 = UnsafeUtils.getInt(address2);

			final int minSize = Math.min(size1,size2);

			address2 = address2 + Integer.BYTES;
			address = address + Integer.BYTES;
			for (int i = 0; i < minSize / Integer.BYTES; i++) {
				int i1 = UnsafeUtils.unsafe.getInt(address + offset);
				int i2 = UnsafeUtils.unsafe.getInt(address2 + offset);
				int compare = Integer.compare(i1, i2);
				if (compare != 0) {
					return compare;
				}
				offset += Integer.BYTES;
			}
			return Integer.compare(size1,size2);
		}

		@Override
		public void Print(long address) {
			int size = UnsafeUtils.getInt(address);
			System.out.print("(");
			for (int i = 0; i < size / Integer.BYTES; i++) {
				System.out.print(UnsafeUtils.unsafe.getInt(address + i+1 *Integer.BYTES));
			}
			System.out.print(")--");
		}
	};

	public static final NovaR DEFAULT_R = new NovaR<Integer>() {
		public
	    Integer apply(Long address) {
	    	int capacity = UnsafeUtils.getInt(address);
	    	address += Integer.BYTES;
	    	int accumulator = 0;
	    	while(capacity > 0 ) {
	    		accumulator += UnsafeUtils.getInt(address);
		    	address += Integer.BYTES;
		    	capacity -= Integer.BYTES;
	    	}
	    	return accumulator;
	    }
		
	};
	public interface GCReader<T> extends Function<Buff,T> {}
	
	public static final GCReader GCR= new GCReader<Integer>() {
		 public Integer apply(Buff buff) {
			 int capacity = buff.capacity;
			 int accumulator = 0;
			 int i = 0;
			 while(capacity > 0) {
				 accumulator += buff.buffer.getInt(i);
				 capacity -= Integer.BYTES;
				 i += Integer.BYTES;
			 }
			 return accumulator;
		}
	};
	
	public static final CopyConstructor<Buff> CC= new CopyConstructor<Buff>() {
		 public Buff Copy(Buff o) {
			Buff toRet = new Buff(o.capacity);
			for (int i = 0; i < o.capacity / Integer.BYTES; i++) { 
				toRet.buffer.putInt(i*Integer.BYTES, o.buffer.getInt(i*Integer.BYTES));
			}
			return toRet;
		}
	};
	

}
