package com.yahoo.oak.SimpleArray;


import java.util.Arrays;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;

public class SA_NoMM {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    
	

    private final NovaS srZ;
	private int size=0;
	private NovaSliceD[] Slices;

	
	
	static public class NovaSliceD extends NovaSlice{

		private boolean deleted;
		
		public NovaSliceD() {
			super(-1, -1, 0);
			deleted = true;
		}
		
		public boolean isDeleted() {
				return deleted? true: false;
		}
		
	}
	
	public SA_NoMM(NovaS srZ){
		Slices = new NovaSliceD[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_NoMM(int capacity,NovaS srZ ){
		Slices = new NovaSliceD[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = new NovaSliceD();
		allocator.allocate(Slices[size], srZ.calculateSize(e));
		srZ.serialize(e, Slices[size].address + Slices[size].offset);
		size++;
		return true;
	}
	
	public <R> R get(int index, NovaR Reader , int threadIDX) {
		R obj = (R) Reader.apply(Slices[index].address+Slices[index].offset);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index].isDeleted())
			allocator.allocate(Slices[index], srZ.calculateSize(obj));
		srZ.serialize(obj, Slices[index].address + Slices[index].offset);
		return true;
	}

	

	public boolean delete(int index, int threadIDX) {
		if(Slices[index].isDeleted())
			return false;
		else {
			allocator.free(Slices[index]);

		}
		return true;
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
}