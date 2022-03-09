package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.NoMM.HarrisLinkedListNoMM;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

public class LL_NoMM_Synch implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.offheap);
	HarrisLinkedListNoMM<Buff,Buff> LL = new HarrisLinkedListNoMM<Buff,Buff>(allocator,
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_NoMM_Synch(long MemCap){
		
	}
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
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public int size() {
    	return LL.Size();
    }
    
    public void clear() {
    	
    }
    
    public void print() {
    	//LL.Print();
    }
}