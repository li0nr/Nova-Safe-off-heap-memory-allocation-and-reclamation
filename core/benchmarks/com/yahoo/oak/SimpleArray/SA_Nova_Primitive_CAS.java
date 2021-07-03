package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.Facade_Nova;
import sun.misc.Unsafe;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.EBR.EBRslice;

public class SA_Nova_Primitive_CAS {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager mng = new NovaManager(allocator);
    
    
	
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

	
	public SA_Nova_Primitive_CAS(NovaS srZ){
		new Facade_Nova(mng);
		refrences = new long[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_Nova_Primitive_CAS(int capacity,NovaS srZ ){
		new Facade_Nova(mng);
		refrences = new long[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == refrences.length) {
			EnsureCap();//might be problematic 
		}
//		Facade_Nova.AllocateSlice(e, refrences_offset + size*Long.BYTES, srZ.calculateSize(e) , threadIDX);
		//Facade_Nova.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX);
		refrences[size]= Facade_Nova.WriteFast(srZ, e, Facade_Nova.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX), threadIDX);
		size++;
		return true;
	}
	
	public long get(int index, int threadIDX) {
		return refrences[index];
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		try {
			if(refrences[index] %2 == 1)
				Facade_Nova.AllocateReusedSlice(refrences,ref_base_offset+index*ref_scale,
						refrences[index],srZ.calculateSize(obj),threadIDX);
			Facade_Nova.WriteFull(srZ, obj, refrences[index], threadIDX);
			return true;
		}catch(NovaIllegalAccess e) {
			return false;
		}
	}
	

	public boolean delete(int index, int threadIDX) {
		if(refrences[index]%2 == 1)
			return false;
		if(Facade_Nova.DeleteReusedSlice(threadIDX, refrences[index], refrences,ref_base_offset+index*ref_scale)){
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
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
}