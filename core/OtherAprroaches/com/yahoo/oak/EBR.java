package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;

public class EBR <T extends EBR_interface>{
	
	private static final long NONE = 0;
	private static final int  EBR_MAX_THREADS = 32;
	private static final int  CLPAD = 16;
	private static final int  RELEASE_LIST_LIMIT = 1024;

    private int[] releasecounter = new int[EBR_MAX_THREADS*2*CLPAD];

    private final AtomicLong eraClock;
    private long[] reservations;
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
	

	public EBR( int maxThreads, NativeMemoryAllocator alloc) {
		//EBR_MAX_THREADS = maxThreads;
		allocator = alloc;
		eraClock = new AtomicLong(1);
		reservations = new long[EBR_MAX_THREADS*CLPAD];
    	for(int it=0; it< EBR_MAX_THREADS; it++) {
    			reservations[it*CLPAD]= (NONE);
    	    	retiredList[it*CLPAD] = new ArrayList<T>();
    	}
	}
	
    public EBRslice allocate(int size) {
    	EBRslice ret = new EBRslice();
    	allocator.allocate(ret, size);
    	return ret;
    }
    
    public EBRslice allocateCAS(int size) {
    	EBRslice ret = new EBRslice();
    	allocator.allocate(ret, size);
		UnsafeUtils.unsafe.fullFence();
    	return ret;
    }
    
	public void start_op(int tid){
		long e = eraClock.get();
		reservations[tid*CLPAD] = e;
		UnsafeUtils.unsafe.fullFence();
	}
		
	public void end_op(int tid){
		reservations[tid*CLPAD] = NONE;
		UnsafeUtils.unsafe.fullFence();
	}
	public void reserve(int tid){
		start_op(tid);
	}
	public void clear(int tid){
		end_op(tid);
	}
	
	void incrementEpoch(){
		eraClock.addAndGet(1);
	}
	
	long getEpoch(){
		return eraClock.get();
	}
	
    public void fastFree(NovaSlice s) {
  	  allocator.free(s);
    }
    
	public void retire(T obj, int tid){
		if(obj== null) return;
		long currEra = eraClock.get();        
		
		retiredList[tid*CLPAD].add(obj);
        
        releasecounter[tid *CLPAD*2]++;
        if(releasecounter[tid *CLPAD*2] == RELEASE_LIST_LIMIT) {
        	incrementEpoch();
        	empty(tid);
            releasecounter[tid *CLPAD*2] = 0;
        }
	}
	
	void empty(int tid){
		
		long minEpoch = Long.MAX_VALUE;
		for (int i = 0; i<EBR_MAX_THREADS; i++){
			long res = reservations[i*CLPAD];
			UnsafeUtils.unsafe.loadFence();
			if(res<minEpoch){
				minEpoch = res;
			}
		}
		// erase safe objects

		ArrayList<T> rlist = retiredList[tid*CLPAD];
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
	
	public void ForceCleanUp() {
		for(int i =0 ; i < EBR_MAX_THREADS; i++) {
	        ArrayList<T> rlist = retiredList[i*CLPAD];
	        EBRslice toDeleteObj;
	        for (int iret = 0; iret < rlist.size(); ) {
            	toDeleteObj = (EBRslice)rlist.get(iret);
            	allocator.free((NovaSlice)toDeleteObj);
                iret++;
            }

		}
	}
	
}
