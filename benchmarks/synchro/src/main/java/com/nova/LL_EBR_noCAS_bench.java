package com.nova;


import com.nova.Buff.Buff;
import com.nova.LL.EBR.LL_EBR_noCAS;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;
import com.nova.synchrobench.contention.benchmark.Parameters;

public class LL_EBR_noCAS_bench implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.offheap);
	LL_EBR_noCAS<Buff,Buff> LL = new LL_EBR_noCAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_EBR_noCAS_bench(long MemCap){		}
	
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key, Buff.DEFAULT_R, tidx);
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
    
    public int size() {
    	return LL.Size();
    }
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	LL = null;
    	allocator.close();
    	allocator = null;
    	
    	allocator = new NativeMemoryAllocator(Parameters.offheap);
    	LL = new LL_EBR_noCAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
    			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    	System.gc();
    }
    
    public void print() {
    	//LL.Print();
    }
}