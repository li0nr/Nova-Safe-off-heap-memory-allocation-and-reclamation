package com.yahoo.oak;


import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.HE.LL_HE_CAS;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_HE_CAS_bench implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	LL_HE_CAS<Buff,Buff> LL = new LL_HE_CAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_HE_CAS_bench(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return LL.contains(key, tidx);
	}
	
    public  boolean put(final Buff key,final Buff value,  int idx) {
    	return LL.add(key,value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
    
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	LL = new LL_HE_CAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
    			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }
}