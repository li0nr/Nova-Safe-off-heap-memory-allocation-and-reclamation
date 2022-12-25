package com.nova;

import com.nova.Buff.Buff;
import com.nova.LL.NoMM.LL_MemSeg_Alloc;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;
import com.nova.synchrobench.contention.benchmark.Parameters;

public class LL_MemSeg_allocator_bench implements CompositionalLL<Buff,Buff>{
	
	MemorySegmentAllocator allocator = new MemorySegmentAllocator(Parameters.offheap);
	LL_MemSeg_Alloc<Buff,Buff> LL = new LL_MemSeg_Alloc<Buff,Buff>(allocator,
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_MemSeg_allocator_bench(long MemCap){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key,Buff.MSR, tidx);
	}
	
    public  boolean put(final Buff key,final Buff value,  int idx) {
    	return LL.add(key,value, idx);
    }
    
    public  boolean putIfAbsent(final Buff key,final Buff value,  int idx) {
    	return LL.putIfAbsentOak(key,value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
        
    public long allocated() {
    	return allocator.allocated();
    }
    
    public int size() {
    	return LL.Size();
    }
    
    public void clear() {
//    	LL = null;
//    	allocator.close();
//    	allocator = null;
//    	allocator = new MemorySegmentAllocator(Parameters.offheap);
//    	LL = new LL_MemSeg_Alloc<Buff,Buff>(allocator,
//    			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    	System.gc();
    }
    
    public void print() {
    	//LL.Print();
    }
}