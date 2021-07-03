package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.CopyConstructor;
import sun.misc.Unsafe;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.Buff.Buff;

public class SA_GC {
	

	private static final int DEFAULT_CAPACITY=10;  
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(Buff[].class);
			slices_scale = UNSAFE.arrayIndexScale(Buff[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final CopyConstructor<Buff> CC;
	private int size=0;
	private Buff[] Slices;

	
	public SA_GC(CopyConstructor CC){
		Slices = new Buff[DEFAULT_CAPACITY];
		this.CC = CC;
	}
	
	public SA_GC(int capacity,CopyConstructor CC ){
		Slices = new Buff[DEFAULT_CAPACITY];
		this.CC = CC;
	}

	public boolean fill(Buff e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = CC.Copy(e);
		size++;
		return true;
	}
	
	public Buff get(int index, int threadIDX) {
		return Slices[index];
	}
	

	public boolean set(int index, Buff obj, int threadIDX)  {
		if(Slices[index] == null) {
			Buff toAdd = CC.Copy(obj); 
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices,
					slices_base_offset+index*slices_scale, null, toAdd))
				return false;
		}
		Slices[index] = CC.Copy(obj);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		Buff toDel = Slices[index];
		if(toDel == null) 
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices,
				slices_base_offset+index*slices_scale, toDel, null))
			return false;
		else return true;
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
}