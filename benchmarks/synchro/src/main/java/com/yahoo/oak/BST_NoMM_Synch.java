package com.yahoo.oak;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.BST.BST_NoMM;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;

public class BST_NoMM_Synch implements CompositionalBST<Buff, Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	BST_NoMM<Buff,Buff>BST = new BST_NoMM<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
			, Buff.DEFAULT_C, Buff.DEFAULT_C,allocator);
	
	
	public BST_NoMM_Synch(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return BST.containsKey(key, tidx);
	}
	
    public  Buff get(final Buff key, int tidx) {
    	return BST.get(key, tidx);
    }
    
    public  Buff put(final Buff key, final Buff value, int idx) {
    	return BST.put(key, value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return BST.remove(key, idx);
    }
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	BST = new BST_NoMM<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
    			, Buff.DEFAULT_C, Buff.DEFAULT_C,allocator);
    }
    public void print() {
    	//BST.Print();
    }
}
