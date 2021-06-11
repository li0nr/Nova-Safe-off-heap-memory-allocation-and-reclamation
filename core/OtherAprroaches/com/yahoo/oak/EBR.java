package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.oak.HazardEras.HEslice;

import sun.misc.Unsafe;

public class EBR <T extends EBR_interface>{
	
	private static final long NONE = -1;
	private static final int  EBR_MAX_THREADS = 32;
	private static final int CLPAD = 33;
	private static final int RELEASE_LIST_LIMIT = 1024;

    private final int             maxThreads= 32;
    private int[] releasecounter = new int[maxThreads*CLPAD];

    private final AtomicLong eraClock;
    private long[] reservations = new long[EBR_MAX_THREADS*CLPAD];
    private final ArrayList<T>[] retiredList= new ArrayList[EBR_MAX_THREADS*CLPAD];//CLPAD is for cache padding
    private final NativeMemoryAllocator allocator;
    	
    
	public class EBRslice extends NovaSlice implements EBR_interface{
		private long epoch;
		
		EBRslice(){
			super(0,0,0);
			epoch = -1;
			}
		
		 public void setEpoch(long Era) {
			 epoch = Era;
		 }
		 
		 public long geteEpoch(){
			 return epoch;
		 }
	}
	

	EBR( int maxThreads, NativeMemoryAllocator alloc) {
		//EBR_MAX_THREADS = maxThreads;
		allocator = alloc;
		eraClock = new AtomicLong(1);
		reservations = new long[EBR_MAX_THREADS*CLPAD];
    	for(int it=0; it< EBR_MAX_THREADS; it++) {
    			reservations[it*CLPAD + 16]= (NONE);
    	    	retiredList[it*CLPAD+16] = new ArrayList<T>();
    	}
	}
	
    public EBRslice allocate(int size) {
    	EBRslice ret = new EBRslice();
    	allocator.allocate(ret, size);
    	return ret;
    }
    
	void start_op(int tid){
		long e = eraClock.get();
		reservations[tid*CLPAD+16] = e;
		UnsafeUtils.unsafe.fullFence();
	}
		
	void end_op(int tid){
		reservations[tid*CLPAD+16] = NONE;
		UnsafeUtils.unsafe.fullFence();
	}
	void reserve(int tid){
		start_op(tid);
	}
	void clear(int tid){
		end_op(tid);
	}
	
	void incrementEpoch(){
		eraClock.addAndGet(1);
	}
	
	long getEpoch(){
		return eraClock.get();
	}
	
	void retire(T obj, int tid){
		if(obj== null) return;
		long currEra = eraClock.get();        
		
		retiredList[tid*CLPAD+ 16].add(obj);
        
        releasecounter[tid *CLPAD + 16]++;
        if(releasecounter[tid *CLPAD + 16] == RELEASE_LIST_LIMIT) {
        	incrementEpoch();
        	empty(tid);
            releasecounter[tid *CLPAD+ 16] = 0;
        }
	}
	
	void empty(int tid){
		
		long minEpoch = Long.MAX_VALUE;
		for (int i = 0; i<EBR_MAX_THREADS; i++){
			long res = reservations[i*CLPAD +16];
			UnsafeUtils.unsafe.loadFence();
			if(res<minEpoch){
				minEpoch = res;
			}
		}
		// erase safe objects

		ArrayList<T> rlist = retiredList[tid*CLPAD+ 16];
		T toDeleteObj = null;
		for (int iret = 0; iret < rlist.size();) {
          	toDeleteObj = rlist.get(iret);
          	if (toDeleteObj.geteEpoch() < minEpoch) {
              	rlist.remove(toDeleteObj);
              	allocator.free((NovaSlice)toDeleteObj);
                  continue;
                  }
              iret++;
              }
	}
	
}
