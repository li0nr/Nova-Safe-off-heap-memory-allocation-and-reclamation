package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.HazardEras;

import sun.misc.Unsafe;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.HazardEras.HEslice;

public class SA_HE_CAS_opt {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final HazardEras _HE= new HazardEras(32, allocator);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(HEslice[].class);
			slices_scale = UNSAFE.arrayIndexScale(HEslice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private HEslice[] Slices;

	
	public SA_HE_CAS_opt(NovaS srZ){
		Slices = new HEslice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_HE_CAS_opt(int capacity,NovaS srZ ){
		Slices = new HEslice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = _HE.allocate(srZ.calculateSize(e));
		srZ.serialize(e, Slices[size].address+Slices[size].offset);
		size++;
		return true;
	}
	
	public <R> R get(int index, NovaR Reader, int threadIDX) {
		try {
			HEslice access = _HE.get_protected(Slices[index],threadIDX);
			if(access == null) {
				_HE.clear(threadIDX);
				return null;
			}
			R obj = (R) Reader.apply(access.address+access.offset);
			_HE.clear(threadIDX);
			return obj;
		}catch(NovaIllegalAccess e) {
			return null;
		}
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
			if(Slices[index]== null) {
				HEslice toEnter = _HE.allocate(srZ.calculateSize(obj));
				if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
					_HE.fastFree(toEnter);
					return false;
				}
			}
			try {
				HEslice access = _HE.get_protected(Slices[index],threadIDX);
				if(access == null) {
					_HE.clear(threadIDX);
					return false;
				}
				srZ.serialize(obj, access.address+access.offset);
				_HE.clear(threadIDX);
				return true;
			}catch(NovaIllegalAccess e) {
				return false;
			}
	}
	
	public boolean delete(int index, int threadIDX) {
		HEslice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, toDel, null))
			return false;
		_HE.retire(threadIDX, toDel);
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