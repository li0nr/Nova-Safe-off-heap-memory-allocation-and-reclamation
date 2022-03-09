package com.yahoo.oak;


import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.EBR.LL_EBR_noCAS_opt;
import com.yahoo.oak.LL.HE.LL_HE_noCAS_opt;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

public class LL_HE_noCAS_opt_bench implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.offheap);
	LL_HE_noCAS_opt<Buff,Buff> LL = new LL_HE_noCAS_opt<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_HE_noCAS_opt_bench(long MemCap){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key,Buff.DEFAULT_R, tidx);
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
    	LL = new LL_HE_noCAS_opt<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
    			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    	System.gc();
    }
    
    public void print() {
    	//LL.Print();
    }
}