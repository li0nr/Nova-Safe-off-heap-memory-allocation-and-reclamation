package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import sun.misc.Unsafe;


public class EBR {
	
	private static final long NONE = Long.MAX_VALUE;

    private final AtomicLong eraClock;
    private final int	[] releasecounter;
    private final AtomicLongArray reservations;
	private final ArrayList[] retiredList;
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
		 
		public long getEpoch(){
			return epoch;
			}
	}
	

	public EBR(NativeMemoryAllocator alloc) {
		//EBR_MAX_THREADS = maxThreads;
		allocator = alloc;
		eraClock = new AtomicLong(0);
		reservations 	= new AtomicLongArray(_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2);
		releasecounter 	= new int[_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2];
		retiredList		= new ArrayList[_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2];
		
    	for(int it=0; it< _Global_Defs.MAX_THREADS; it++) {
    			reservations.set(it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);
    	    	retiredList[it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]
    	    			= new ArrayList();
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
		reservations.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,e);
		UnsafeUtils.unsafe.fullFence();
	}
		
	public void end_op(int tid){
		reservations.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);
		UnsafeUtils.unsafe.storeFence();
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
    
	public <T extends EBR_interface> void retire(T obj, int tid){
		if(obj== null) return;
		long currEra = eraClock.get();        
		obj.setEpoch(currEra);
		retiredList[tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING].add(obj);
        
        releasecounter[tid *_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]++;
        if(releasecounter[tid *_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] == _Global_Defs.RELEASE_LIST_LIMIT) {
        	incrementEpoch();
        	empty(tid);
            releasecounter[tid *_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] = 0;
        }
	}
	
	<T extends EBR_interface> void empty(int tid){
		
		long minEpoch = Long.MAX_VALUE;
		for (int i = 0; i<_Global_Defs.MAX_THREADS; i++){
			long res = reservations.get(i*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING);
			if(res<minEpoch){
				minEpoch = res;
			}
		}
		UnsafeUtils.unsafe.loadFence();

		// erase safe objects
		ArrayList<T> rlist = retiredList[tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
		T toDeleteObj = null;
		for (int iret = 0; iret < rlist.size();) {
          	toDeleteObj = rlist.get(iret);
          	if (toDeleteObj.getEpoch() < minEpoch) {
              	rlist.remove(toDeleteObj);
              	allocator.free(new NovaSlice((NovaSlice)toDeleteObj));
              	continue;
              	}
          	iret++;
          	}
		}
	
	public <T extends EBR_interface> void ForceCleanUp() {
		for(int i =0 ; i < _Global_Defs.MAX_THREADS; i++) {
	        ArrayList<T> rlist = retiredList[i*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
	        EBRslice toDeleteObj;
	        for (int iret = 0; iret < rlist.size(); ) {
            	toDeleteObj = (EBRslice)rlist.get(iret);
            	allocator.free((NovaSlice)toDeleteObj);
                iret++;
            }

		}
	}
	
}
