package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.HarrisLinkedList;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

public class LL_GC implements CompositionalLL<Buff,Buff>{

	HarrisLinkedList<Buff,Buff> LL = new HarrisLinkedList<Buff,Buff>(Buff.CC,Buff.CC);
	
	public LL_GC(){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key, Buff.GCR, tidx);
	}
	
    public  boolean put(final Buff key,final Buff value,  int idx) {
    	return LL.add(key,value, idx);
    }
    
    public  boolean Fill(final Buff key,final Buff value,  int idx) {
    	return LL.Fill(key,value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
        
    public long allocated() {
    	return 0;
    }
    
    public int Size() {
    	return LL.Size();
    }
    
    public void clear() {

    }
    
    public void print() {
    	//LL.Print();
    }
}