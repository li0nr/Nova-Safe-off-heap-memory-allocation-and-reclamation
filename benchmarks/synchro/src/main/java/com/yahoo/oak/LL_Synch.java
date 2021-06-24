package com.yahoo.oak;

import java.util.Iterator;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.HarrisLinkedList;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_Synch implements CompositionalLL<Buff>{

	HarrisLinkedList<Buff> LL = new HarrisLinkedList<Buff>(Buff.CC);
	
	public LL_Synch(){
		
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
    	return 0;
    }
    
    public void clear() {
    	LL = new HarrisLinkedList<Buff>(Buff.CC);
    }
    
    public void print() {
    	//LL.Print();
    }
}