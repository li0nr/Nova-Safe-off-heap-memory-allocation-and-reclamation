package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.NoMM.HarrisLinkedListNoMM;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_NoMM_Synch implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	HarrisLinkedListNoMM<Buff,Buff> LL = new HarrisLinkedListNoMM<Buff,Buff>(allocator,
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_NoMM_Synch(){
		
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
    	LL = new HarrisLinkedListNoMM<Buff,Buff>(allocator,
    			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }
}