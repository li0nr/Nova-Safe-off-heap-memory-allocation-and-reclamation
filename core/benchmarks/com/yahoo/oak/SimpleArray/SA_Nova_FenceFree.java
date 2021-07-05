package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Nova_FenceFree;

import sun.misc.Unsafe;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManagerNoTap;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;

public class SA_Nova_FenceFree {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManagerNoTap mng = new NovaManagerNoTap(allocator);
    
    
	
    static final long ref_base_offset;
    static final long ref_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			ref_base_offset = UNSAFE.arrayBaseOffset(long[].class);
			ref_scale = UNSAFE.arrayIndexScale(long[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private long[] refrences;

	
	public SA_Nova_FenceFree(NovaS srZ){
		new Facade_Nova_FenceFree(mng);
		refrences = new long[DEFAULT_CAPACITY];
		for(int i=0; i <DEFAULT_CAPACITY ; i++)
			refrences[i] = 1;
		this.srZ = srZ;
	}
	
	public SA_Nova_FenceFree(int capacity,NovaS srZ ){
		new Facade_Nova_FenceFree(mng);
		refrences = new long[capacity];
		refrences = new long[DEFAULT_CAPACITY];
		for(int i=0; i <DEFAULT_CAPACITY ; i++)
			refrences[i] = 1;
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == refrences.length) {
			EnsureCap();//might be problematic 
		}
		refrences[size]= Facade_Nova_FenceFree.WriteFast(srZ, e, 
				Facade_Nova_FenceFree.AllocateReusedSlice(refrences,ref_base_offset+size*ref_scale,
						refrences[size],srZ.calculateSize(e),threadIDX), threadIDX);
		size++;
		return true;
	}


	public <R> R get(int index, NovaR Reader, int threadIDX) {
		try {
			return  Facade_Nova_FenceFree.Read(Reader, refrences[index]);
		}catch(NovaIllegalAccess e) {
			return null;
		}	
	}
	
	public <T> boolean set(int index, T obj, int threadIDX)  {
		try {
			if(refrences[index] %2 == 1)
				Facade_Nova_FenceFree.AllocateReusedSlice(refrences,ref_base_offset+index*ref_scale,
						refrences[index],srZ.calculateSize(obj),threadIDX);
			Facade_Nova_FenceFree.WriteFull(srZ, obj, refrences[index], threadIDX);
			return true;
		}catch(NovaIllegalAccess e) {
			return false;
		}
	}
	

	public boolean delete(int index, int threadIDX) {
		if(refrences[index]%2 == 1)
			return false;
		if(Facade_Nova_FenceFree.DeleteReusedSlice(threadIDX, refrences[index], refrences,ref_base_offset+index*ref_scale)){
			return true;
		}
		return false;
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = refrences.length *2;
		refrences = Arrays.copyOf(refrences, newSize);
		for(int i = refrences.length/2 ; i <newSize ; i++)
			refrences[i] = 1;
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
}