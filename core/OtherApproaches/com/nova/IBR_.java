package com.nova;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class IBR_ {

	private static final long NONE = Long.MAX_VALUE;

    private final AtomicLong eraClock;
    private final int	[] releasecounter;
    private final AtomicLongArray reservationsHI;
    private final AtomicLongArray reservationsLO;

	private final ArrayList[] retiredList;
    private final NativeMemoryAllocator allocator;
    	
    
	public class IBRslice extends NovaSlice implements HazardEras_interface{
		private long bornEra;
		private long deadEra;
		
		IBRslice(long Era){
			super(0,0,0);
			bornEra = Era;
			deadEra = -1;
		}
		
		 public void setDeleteEra(long Era){
			 deadEra = Era;
		 }
		 
		 public void setEra(long Era) {
			 bornEra = Era;
		 }

		 public long getnewEra() {
			 return bornEra;
		 }
		 
		 public long getdelEra() {
			 return deadEra;
		 }
	}
	

	public IBR_(NativeMemoryAllocator alloc) {
		//EBR_MAX_THREADS = maxThreads;
		allocator = alloc;
		eraClock = new AtomicLong(0);
		reservationsHI 	= new AtomicLongArray(_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2);
		reservationsLO 	= new AtomicLongArray(_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2);

		releasecounter 	= new int[_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2];
		retiredList		= new ArrayList[_Global_Defs.MAX_THREADS*_Global_Defs.CACHE_PADDING*2];
		
    	for(int it=0; it< _Global_Defs.MAX_THREADS; it++) {
    			reservationsHI.set(it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);
    			reservationsLO.set(it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);

    	    	retiredList[it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]
    	    			= new ArrayList();
    	}
	}
	
    public IBRslice allocate(int size) {
    	IBRslice ret = new IBRslice(getEpoch());
    	allocator.allocate(ret, size);
    	return ret;
    }
    
    public IBRslice get_protected(IBRslice obj, int tid) {
   	 long prevEra = reservationsHI.get(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING);
   	 while (true) {
   		 //T loadedOBJ= obj;//memory_order_seq_cst	in c++ is mapped to 
		    				//A load operation with this memory order performs an acquire operation,
		    				//a store performs a release operation,
		    				//and read-modify-write performs both an acquire operation and a release operation,
		    				//plus a single total order exists in which all threads observe all modifications in the same order
   		 if(obj != null && obj.deadEra != -1)
   			 return null;

   		 UnsafeUtils.unsafe.loadFence();
   		 long era = eraClock.get();
   		 UnsafeUtils.unsafe.loadFence(); ////must be here this is aquvilent to the acquire 

   		 if (era == prevEra) return obj ;
   		 UnsafeUtils.unsafe.fullFence();
   		 reservationsHI.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING, era); //TODO he must be volatile
   		 prevEra = era;
		}
   }
    
	public void start_op(int tid){
		long e = eraClock.get();
		reservationsHI.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,e);
		reservationsLO.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,e);

		UnsafeUtils.unsafe.fullFence();
	}
		
	public void end_op(int tid){
		reservationsHI.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);
		reservationsLO.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);

		UnsafeUtils.unsafe.storeFence();
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
	
	
	public void retire(int mytid, HazardEras_interface obj) {
		retire(mytid,obj,obj.getnewEra());
	}
	
    public <T> void retire(int mytid, HazardEras_interface obj, long BirthEra) {
        long currEra = eraClock.get();
        obj.setDeleteEra(currEra);
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
        rlist.add(obj);
        releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]++;
        if(releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] == _Global_Defs.RELEASE_LIST_LIMIT) {
            if (eraClock.get() == currEra) eraClock.getAndAdd(1);
            empty(mytid);
            releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] = 0;
        }

    }
    
    public void empty(int mytid){
    	long[] upperEpochs = new long[_Global_Defs.MAX_THREADS];
    	long[] lowerEpochs = new long[_Global_Defs.MAX_THREADS];
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
        HazardEras_interface toDeleteObj;
		for (int i = 0; i < _Global_Defs.MAX_THREADS; i++){
			//sequence matters.
			upperEpochs[i] = reservationsHI.get(i*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING);
			lowerEpochs[i] = reservationsLO.get(i*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING);
		}
        for (int iret = 0; iret < rlist.size();) {
        	toDeleteObj = (HazardEras_interface)rlist.get(iret);
            if (canDelete(lowerEpochs, upperEpochs, toDeleteObj.getnewEra(), toDeleteObj.getdelEra(), mytid)) {
            	rlist.remove(toDeleteObj);
            	allocator.free(new NovaSlice((NovaSlice)toDeleteObj));
                continue;
            }
            iret++;
        }
		
    }
    
    private boolean  canDelete(long[] lower, long[] upper, long born, long dead,  int mytid) {
  	  for (int tid = 0; tid < _Global_Defs.MAX_THREADS; tid++) {
          if(upper[tid] >= born && lower[tid] <= dead)
        	  return false;
          }
  	  return true;
  	  }
    	
}
