package com.yahoo.oak.SimpleArray;


import java.util.Arrays;
import java.util.function.Function;

import sun.misc.Unsafe;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;
import com.yahoo.oak.UnsafeUtils;

public class SA_NoMM2 {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(NovaSlice[].class);
			slices_scale = UNSAFE.arrayIndexScale(NovaSlice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private NovaSlice[] Slices;

	
	public SA_NoMM2(NovaS srZ){
		Slices = new NovaSlice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_NoMM2(int capacity,NovaS srZ ){
		Slices = new NovaSlice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = new NovaSlice(0, 0, 0);
		allocator.allocate(Slices[size], srZ.calculateSize(e));
		srZ.serialize(e, Slices[size].address+Slices[size].offset);
		size++;
		return true;
	}
	
	public <R> R get(int index, Function Reader, int threadIDX) {
		NovaSlice toRead = Slices[index];
		if(toRead == null) return null;
		R obj = (R) Reader.apply(toRead.address+toRead.offset);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index] == null) {
			NovaSlice toEnter = new NovaSlice(0, 0, 0);
			allocator.allocate(Slices[size], srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				allocator.free(toEnter);
				return false;
			}
		}
		NovaSlice toEnter = Slices[index];
		if(toEnter == null) {
			return false;
		}
		srZ.serialize(obj, toEnter.address+toEnter.offset);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		NovaSlice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale,toDel, null))
			return false;
		allocator.free(toDel);
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