package com.yahoo.oak;

import java.util.Iterator;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.HarrisLinkedListHE;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_HE_Synch implements CompositionalLL<Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	HarrisLinkedListHE<Buff> LL = new HarrisLinkedListHE<Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_HE_Synch(){
		
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
    
    public  Iterator<Buff> iterator(int idx){
    	return LL.iterator(idx);
    }
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	LL = new HarrisLinkedListHE<Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }
}