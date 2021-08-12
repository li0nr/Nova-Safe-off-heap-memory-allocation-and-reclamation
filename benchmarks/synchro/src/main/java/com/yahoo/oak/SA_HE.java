package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_HE_CAS_opt;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalSA;


public class SA_HE implements CompositionalSA<Buff>{
	SA_HE_CAS_opt SA;
	
	public SA_HE(int size){
		 SA = new SA_HE_CAS_opt(size, Buff.DEFAULT_SERIALIZER);
	}
    public  boolean fill(final Buff value, int idx) {
    	return SA.fill( value, idx);
    }
    
    public  boolean ParallelFill(int size) {
		return SA.ParallelFill(size);
    }
    
	public  Integer get(int index, int idx) {
    	return SA.get(index, Buff.DEFAULT_R, idx);
    }
	
    public  boolean put(final Buff value,  int index, int idx) {
    	return SA.set(index, value, idx);
    }
    
    public  boolean remove(int index, int idx) {
    	return SA.delete(index, idx);
    }
    
    public long allocated() {
    	return SA.getAlloc().allocated();

    }
	
    public void clear(int size) {
    	SA.close();
    	SA = new SA_HE_CAS_opt(size,Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }

}
