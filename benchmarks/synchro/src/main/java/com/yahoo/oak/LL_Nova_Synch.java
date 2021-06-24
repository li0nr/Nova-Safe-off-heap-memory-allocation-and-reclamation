package com.yahoo.oak;

import java.util.Iterator;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.HarrisLinkedListNova;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_Nova_Synch implements CompositionalLL<Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	NovaManager mng = new NovaManager(allocator);
	HarrisLinkedListNova<Buff> LL = new HarrisLinkedListNova<Buff>(mng, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_Nova_Synch(){
		
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
    	mng = new NovaManager(allocator);
    	LL = new HarrisLinkedListNova<Buff>(mng, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }
}