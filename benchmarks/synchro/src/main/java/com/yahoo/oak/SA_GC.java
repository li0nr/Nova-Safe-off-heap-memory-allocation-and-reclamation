package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalSA;



public class SA_GC implements CompositionalSA<Buff>{
	com.yahoo.oak.SimpleArray.SA_GC SA;
	
	public SA_GC(int size){
		SA = new com.yahoo.oak.SimpleArray.SA_GC(size, Buff.CC);
	}
    public  boolean fill(final Buff value, int idx) {
    	return SA.fill( value, idx);
    }
    
    public  Integer get( int index, int idx) {
    	return SA.get(index, Buff.GCR, idx);
    }
    
    public  boolean put(final Buff value,  int index, int idx) {
    	return SA.set(index, value, idx);
    }
    
    public  boolean remove(int index, int idx) {
    	return SA.delete(index, idx);
    }
    
    public long allocated() {
    	return 0;
    }
	
    public void clear(int size) {
    	SA = new com.yahoo.oak.SimpleArray.SA_GC(size, Buff.CC);
    }
    
    public void print() {
    	//LL.Print();
    }

}
