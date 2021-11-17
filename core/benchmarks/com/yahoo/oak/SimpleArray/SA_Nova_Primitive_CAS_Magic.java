package com.yahoo.oak.SimpleArray;


import java.util.Arrays;

import com.yahoo.oak.Facade_Nova_MagicNumber;
import sun.misc.Unsafe;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.ParamBench;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.EBR.EBRslice;

public class SA_Nova_Primitive_CAS_Magic {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
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

	
	public SA_Nova_Primitive_CAS_Magic(NovaS srZ){
		new Facade_Nova_MagicNumber(mng);
		refrences = new long[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_Nova_Primitive_CAS_Magic(int capacity,NovaS srZ ){
		new Facade_Nova_MagicNumber(mng);
		refrences = new long[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == refrences.length) {
			EnsureCap();//might be problematic 
		}
//		Facade_Nova_MagicNumber.AllocateSlice(e, refrences_offset + size*Long.BYTES, srZ.calculateSize(e) , threadIDX);
		//Facade_Nova_MagicNumber.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX);
		refrences[size]= Facade_Nova_MagicNumber.WriteFast(srZ, e, Facade_Nova_MagicNumber.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX), threadIDX);
		size++;
		return true;
	}
	
	public <R> R get(int index, NovaR Reader, int threadIDX) {
		try {
			return (R)Facade_Nova_MagicNumber.Read(Reader, refrences[index]);
		}catch(NovaIllegalAccess e) {
			return null;
		}
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(refrences[index] %2 == 1)
			Facade_Nova_MagicNumber.AllocateReusedSlice(refrences,ref_base_offset+index*ref_scale,//reused not correct!
					refrences[index],srZ.calculateSize(obj),threadIDX);
		return Facade_Nova_MagicNumber.WriteFull(srZ, obj, refrences[index], threadIDX) != -1 ? true : false; 
	}
	
	public <T> T get(NovaR<T> Reader, int index, int threadIDX)  {
		try {
			return  (T)Facade_Nova_MagicNumber.Read(Reader, refrences[index]);
		}catch(NovaIllegalAccess e) {
			return null;
		}
	}
	

	public boolean delete(int index, int threadIDX) {
		return Facade_Nova_MagicNumber.DeleteReusedSlice(threadIDX, refrences[index], refrences,ref_base_offset+index*ref_scale);
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