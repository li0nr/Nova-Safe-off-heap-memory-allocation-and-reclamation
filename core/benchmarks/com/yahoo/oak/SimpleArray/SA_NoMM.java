package com.yahoo.oak.SimpleArray;


import java.util.Arrays;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.EBR.EBRslice;

import sun.misc.Unsafe;

public class SA_NoMM {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(NovaSliceD[].class);
			slices_scale = UNSAFE.arrayIndexScale(NovaSliceD[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }

    private final NovaS srZ;
	private int size=0;
	private NovaSlice[] Slices;

	
	
	static public class NovaSliceD extends NovaSlice{

		private boolean deleted;
		
		public NovaSliceD() {
			super(-1, -1, 0);
			deleted = false;
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
		NovaSlice toRead = Slices[index];
		if(toRead == null)
			return null;
		R obj = (R) Reader.apply(toRead.address+toRead.offset);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index] == null) {
			NovaSlice toEnter = new NovaSlice(0, 0, 0);
			allocator.allocate(toEnter, srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				allocator.free(toEnter);
				return false;
			}
		}
		NovaSlice toEnte = Slices[index] ;
		if(toEnte == null)			
			return false;
		srZ.serialize(obj, toEnte.address + toEnte.offset);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		NovaSlice toDel = Slices[index];
		if(toDel != null)
			return false;
		if(UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale,toDel, null)) {
			allocator.free(toDel);
			return true;
		}
		return false;
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