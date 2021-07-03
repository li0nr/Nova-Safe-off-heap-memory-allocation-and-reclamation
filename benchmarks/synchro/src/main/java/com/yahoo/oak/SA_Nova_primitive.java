package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_Nova_Primitive_CAS;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalSA;


public class SA_Nova_primitive implements CompositionalSA<Buff>{
	SA_Nova_Primitive_CAS SA = new SA_Nova_Primitive_CAS(Buff.DEFAULT_SERIALIZER);
	
	public SA_Nova_primitive(){
		
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
    	return SA.getAlloc().allocated();

    }
	
    public void clear() {
    	SA = new SA_Nova_Primitive_CAS(Buff.DEFAULT_SERIALIZER);
    }
    
    public void print() {
    	//LL.Print();
    }

}
