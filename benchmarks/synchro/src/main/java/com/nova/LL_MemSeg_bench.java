package com.nova;

import com.nova.Buff.Buff;
import com.nova.LL.NoMM.LL_MemSeg;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;

public class LL_MemSeg_bench implements CompositionalLL<Buff,Buff>{
	
	LL_MemSeg<Buff,Buff> LL = new LL_MemSeg<Buff,Buff>(
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_MemSeg_bench(long MemCap){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key,Buff.MSR, tidx);
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
    	return -1;
    }
    
    public int size() {
    	return LL.Size();
    }
    
    public void clear() {
    	System.gc();
    }
    
    public void print() {
    	//LL.Print();
    }
}