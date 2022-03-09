package com.yahoo.oak.NotUsed;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.Nova.LL_Nova_primitive_CAS;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;

public class LL_Nova_Synch implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
	NovaManager mng = new NovaManager(allocator);
	LL_Nova_primitive_CAS<Buff,Buff> LL = new LL_Nova_primitive_CAS<Buff,Buff>(mng, 
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_Nova_Synch(){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key, Buff.DEFAULT_R, tidx);
	}
	
    public  boolean put(final Buff key,final Buff value,  int idx) {
    	return LL.add(key,value, idx);
    }
    
    public  boolean putIfAbsent(final Buff key,final Buff value,  int idx) {
    	return LL.add(key,value, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
        
    public long allocated() {
    	return allocator.allocated();
    }
    
    public void clear() {
    	allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
    	mng = new NovaManager(allocator);
    	LL  = new LL_Nova_primitive_CAS<Buff,Buff>(mng, 
    			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    }
    
    public int size() {return 0;}
    public void print() {}
}