package com.yahoo.oak;

import com.yahoo.oak.BST.BST;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;

public class BST_GC implements CompositionalBST<Buff, Buff>{
	
	BST<Buff,Buff>BST = new BST<Buff,Buff>(Buff.CC,Buff.CC);
	
	
	public BST_GC(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return BST.containsKey(key);
	}
	
    public  Buff get(final Buff key, int tidx) {
    	return BST.get(key);
    }
    
    public  boolean put(final Buff key, final Buff value, int idx) {
    	return BST.put(key, value);
    }
    
    public  boolean Fill(final Buff key, final Buff value, int idx) {
    	return BST.Fill(key, value);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return BST.remove(key);
    }
    
    public long allocated() {
    	return 0;
    }
    
    public void clear() {
    	BST = new BST<Buff,Buff>(Buff.CC,Buff.CC);
    }
    public void print() {
    //	BST.Print();
    }
}
