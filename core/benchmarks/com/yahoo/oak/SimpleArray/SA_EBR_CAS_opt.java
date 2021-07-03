package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.EBR;
import com.yahoo.oak.EBR.EBRslice;
import sun.misc.Unsafe;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;

public class SA_EBR_CAS_opt {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final EBR _EBR = new EBR<>(allocator);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(EBRslice[].class);
			slices_scale = UNSAFE.arrayIndexScale(EBRslice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private EBRslice[] Slices;

	
	public SA_EBR_CAS_opt(NovaS srZ){
		Slices = new EBRslice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_EBR_CAS_opt(int capacity,NovaS srZ ){
		Slices = new EBRslice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = _EBR.allocate(srZ.calculateSize(e));
		srZ.serialize(e, Slices[size].address+Slices[size].offset);
		size++;
		return true;
	}
	
	public EBRslice get(int index, int threadIDX) {
		return Slices[index];
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		EBRslice toEnter = Slices[index];
		if(toEnter== null) {
			//EBRslice toEnter = _EBR.allocateCAS(srZ.calculateSize(obj));
			toEnter = _EBR.allocate(srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				_EBR.fastFree(toEnter);
				return false;
			}
		}
		_EBR.start_op(threadIDX);
		if(Slices[index] == null) {
			_EBR.end_op(threadIDX);
			return false;
		}
		srZ.serialize(obj, toEnter.address+toEnter.offset);
		_EBR.end_op(threadIDX);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		EBRslice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale,toDel, null))
			Slices[index] = null;
		else 
			return false;
		_EBR.retire(toDel, threadIDX);
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