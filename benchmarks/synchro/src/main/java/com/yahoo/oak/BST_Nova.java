package com.yahoo.oak;


import com.yahoo.oak.BST.BST_Nova_Long;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

public class BST_Nova implements CompositionalBST<Buff, Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
	NovaManager novaManager = new NovaManager(allocator);
	BST_Nova_Long<Buff,Buff>BST = new BST_Nova_Long<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
			, Buff.DEFAULT_C, Buff.DEFAULT_C,novaManager);
	
	
	public BST_Nova(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return BST.containsKey(key, tidx);
	}
	
    public  Buff get(final Buff key, int tidx) {
    	return null;
    	//return BST.get(key, tidx);
    }
    
    public boolean put(final Buff key, final Buff value, int idx) {
    	return BST.put(key, value, idx);
    }
    
    public boolean Fill(final Buff key, final Buff value, int idx) {
    	return BST.Fill(key, value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return BST.remove(key, idx);
    }
    
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator.close();
    	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
    	novaManager = new NovaManager(allocator);
    	BST = new BST_Nova_Long<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
    			, Buff.DEFAULT_C, Buff.DEFAULT_C,novaManager);
    }
    public void print() {
    	BST.Print();
    }
    
}
