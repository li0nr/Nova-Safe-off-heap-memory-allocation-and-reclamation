package com.yahoo.oak;


import java.util.Arrays;


public class List_Nova implements ListInterface{

	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager novaManager = new NovaManager(allocator);
    
	



	long[] ArrayOfFacades;
	private int size=0;
	

	
	public List_Nova(){
		new Facade_Nova(novaManager);
		ArrayOfFacades=new long[DEFAULT_CAPACITY];
	}
	
	public List_Nova(int capacity){
		new Facade_Nova(novaManager);
		ArrayOfFacades=new long[capacity];
	}

	public boolean add(Long e,int idx) {
		if(size == ArrayOfFacades.length) {
			EnsureCap();
		}

		if(ArrayOfFacades[size] == 0)
			ArrayOfFacades[size]= Facade_Nova.AllocateSlice(Long.BYTES,idx);
		Facade_Nova.WriteFast(DEFAULT_SERIALIZER,e,ArrayOfFacades[size], idx);
	    size++;
	    return true;
	}
	
	public long get(int i, int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
		return Facade_Nova.Read(DEFAULT_R , ArrayOfFacades[i]);
	}
	
	public boolean set(int index, long e, int idx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		 if(Facade_Nova.WriteFull(DEFAULT_SERIALIZER,e,ArrayOfFacades[index],idx) != -1)
			 return true;
		 else 
			 return false;
	}
	
	public void allocate(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		ArrayOfFacades[index] = Facade_Nova.AllocateSlice(8, threadidx);
	}
	
	public boolean delete(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		return  Facade_Nova.Delete(threadidx, ArrayOfFacades[index]);
	}
	
	
	public int getSize(){
		return size;
	}
	
   public void remove(int index, int idx) {
        Facade_Nova.Delete(idx, ArrayOfFacades[index]);
        
    }
	
	private void EnsureCap() {
		int newSize = ArrayOfFacades.length *2;
		ArrayOfFacades = Arrays.copyOf(ArrayOfFacades, newSize);
	}
	
	public long getUsedMem() {
		return novaManager.allocated();
	}
	
	public long getAllocatedMem() {
		return allocator.numOfAllocatedBlocks()*1024*1024;
	}
	
	
	public static final NovaS<Long> DEFAULT_SERIALIZER = new NovaS<Long>() {
		@Override
		public void serialize(Long object, long output) {
			UnsafeUtils.putLong(output, (long)object);
		}
		
		@Override
		public void serialize(long source, long output) {
			UnsafeUtils.putInt(output, UnsafeUtils.getInt(source));
		}

		@Override
		public Long deserialize(long input) {
			return UnsafeUtils.getLong(input);
		}

		@Override
		public int calculateSize(Long object) {
			return 8;
		}
	};
	
	public static final NovaR<Long> DEFAULT_R = new NovaR<Long>() {
		public
		Long apply(Long address) {
			return UnsafeUtils.getLong(address);
	    }
	};
 
	@Override
	public void close()  {
		novaManager.close();
	}
	

public  static void main(String[] args)throws java.io.IOException {
	List_Nova s = new List_Nova();
	for(int i=0; i<100; i++) {
		s.add((long)i,0);
		}
	for(int i=0; i<100; i++) {
		s.set(i,(long)i,0);
		}
	s.close();
	}

}

