package com.yahoo.oak;
//package com.yahoo.oak;
//
//
//import com.yahoo.oak.Buff.Buff;
//import com.yahoo.oak.LL.HE.LL_HE_CAS;
//import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
//import com.yahoo.oak.synchrobench.contention.benchmark.Parameters;
//
//public class LL_HE_CAS_bench implements CompositionalLL<Buff,Buff>{
//	
//	NativeMemoryAllocator allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
//	LL_HE_CAS<Buff,Buff> LL = new LL_HE_CAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
//			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
//	
//	public LL_HE_CAS_bench(){
//		
//	}
//	public Integer containsKey(final Buff key, int tidx) {return 0;	}
//
//	
//    public  boolean put(final Buff key,final Buff value,  int idx) {
//    	return LL.add(key,value, idx);
//    }
//    
//    public  boolean remove(final Buff key, int idx) {
//    	return LL.remove(key, idx);
//    }
//    
//    
//    public long allocated() {
//    	return allocator.allocated();
//    }
//    
//    public void clear() {
//    	allocator = new NativeMemoryAllocator(Parameters.MAXSIZE);
//    	LL = new LL_HE_CAS<Buff,Buff>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
//    			,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
//    }
//    
//    public void print() {
//    	//LL.Print();
//    }
//}