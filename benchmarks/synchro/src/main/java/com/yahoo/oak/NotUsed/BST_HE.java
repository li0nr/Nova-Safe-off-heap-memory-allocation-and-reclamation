package com.yahoo.oak.NotUsed;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.BST.BST_HE_;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

public class BST_HE implements CompositionalBST<Buff, Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
	BST_HE_<Buff,Buff>BST = new BST_HE_<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
			, Buff.DEFAULT_C, Buff.DEFAULT_C,allocator);
	
	
	public BST_HE(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return BST.containsKey(key, tidx);
	}
	
    public  Buff get(final Buff key, int tidx) {
    	return BST.get(key, tidx,Buff.DEFAULT_R);
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
    	allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
    	BST = new BST_HE_<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
    			, Buff.DEFAULT_C, Buff.DEFAULT_C,allocator);
    }
    
    public void print() {
    	BST.Print();
    }
}
