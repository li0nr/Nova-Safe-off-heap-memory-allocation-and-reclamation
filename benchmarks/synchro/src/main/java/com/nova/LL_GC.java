package com.nova;

import com.nova.Buff.Buff;
import com.nova.LL.HarrisLinkedList;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;

public class LL_GC implements CompositionalLL<Buff,Buff>{

	HarrisLinkedList<Buff,Buff> LL = new HarrisLinkedList<Buff,Buff>(Buff.CC,Buff.CC);
	
	public LL_GC(long MemCap){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key, Buff.GCR, tidx);
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
    	return 0;
    }
    
    public int size() {
    	return LL.Size();
    }
    
    public void clear() {
    	LL = null;
    	System.gc();
    	LL = new HarrisLinkedList<Buff,Buff>(Buff.CC,Buff.CC);
    }
    
    public void print() {
    	//LL.Print();
    }
}