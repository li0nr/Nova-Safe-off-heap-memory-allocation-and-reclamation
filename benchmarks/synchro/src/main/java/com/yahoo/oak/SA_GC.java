package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_EBR_CAS_opt;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalSA;



public class SA_GC implements CompositionalSA<Buff>{
	com.yahoo.oak.SimpleArray.SA_GC SA = new 	com.yahoo.oak.SimpleArray.SA_GC(Buff.CC);
	
	public SA_GC(){
		
	}
    public  boolean fill(final Buff value, int idx) {
    	return SA.fill( value, idx);
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
	
    public void clear() {
    	SA = new com.yahoo.oak.SimpleArray.SA_GC(Buff.CC);
    }
    
    public void print() {
    	//LL.Print();
    }

}
