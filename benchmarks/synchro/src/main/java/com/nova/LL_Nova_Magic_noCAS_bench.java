package com.nova;

import com.nova.Buff.Buff;
import com.nova.LL.Nova.LL_Nova_primitive_noCAS_Magic;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;
import com.nova.synchrobench.contention.benchmark.Parameters;

public class LL_Nova_Magic_noCAS_bench implements CompositionalLL<Buff,Buff>{
	
	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.offheap);
	NovaManager mng = new NovaManager(allocator);
	LL_Nova_primitive_noCAS_Magic<Buff,Buff> LL = new LL_Nova_primitive_noCAS_Magic<Buff,Buff>(mng, 
			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	
	public LL_Nova_Magic_noCAS_bench(long MemCap){
		
	}
	public Integer containsKey(final Buff key, int tidx) {
		return LL.get(key,Buff.DEFAULT_R, tidx);
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
    	return allocator.allocated();
    }
    
    public int size() {
    	return LL.Size();
    }
    
    public void clear() {
    	LL = null;
    	allocator.close();
    	allocator = null;
    	
    	allocator = new NativeMemoryAllocator(Parameters.offheap);
    	NovaManager mng = new NovaManager(allocator);
    	LL = new LL_Nova_primitive_noCAS_Magic<Buff,Buff>(mng, 
    			Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
    	System.gc();
    }
    
    public void print() {
    	//LL.Print();
    }
}