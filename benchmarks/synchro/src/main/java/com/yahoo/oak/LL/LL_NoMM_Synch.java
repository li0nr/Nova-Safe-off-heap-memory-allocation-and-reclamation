package com.yahoo.oak.LL;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.synchrobench.contention.abstractions.Compositional;

public class LL_NoMM_Synch implements Compositional<Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	HarrisLinkedListNoMM<Buff> LL = new HarrisLinkedListNoMM<Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_NoMM_Synch(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return LL.contains(key, tidx);
	}
	
    public  boolean put(final Buff key,  int idx) {
    	return LL.add(key, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	LL = new HarrisLinkedListNoMM<Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }
}