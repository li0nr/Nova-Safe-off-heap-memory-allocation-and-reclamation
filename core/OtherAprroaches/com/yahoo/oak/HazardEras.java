package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;

public class HazardEras{

	private static final long NONE = 0;
	private static final int  HE_MAX_THREADS = 32;
	private static final int  CLPAD = 16;
	private static final int  RELEASE_LIST_LIMIT = 1024;

    private int[] releasecounter = new int[HE_MAX_THREADS*2*CLPAD];

    private final AtomicLong eraClock;
    private long[] he;
    private final ArrayList<HazardEras_interface>[] retiredList= new ArrayList[HE_MAX_THREADS*CLPAD];//CLPAD is for cache padding
    private final NativeMemoryAllocator allocator;
	
	
	public class HEslice extends NovaSlice implements HazardEras_interface{
		private long bornEra;
		private long deadEra;
		
		HEslice(long Era){
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
		 
	public HazardEras(int maxThreads, NativeMemoryAllocator alloc) {
		allocator = alloc;
		eraClock = new AtomicLong(NONE);
		he = new long[HE_MAX_THREADS*CLPAD];
    	for(int it=0; it< HE_MAX_THREADS; it++) {
			he[it*CLPAD]= (NONE);
    		retiredList[it*CLPAD] = new ArrayList<HazardEras_interface>();
    		}
    	}

    long getEra() {
        return eraClock.get();
        }

    
    public HEslice allocate(int size) {
    	HEslice ret = new HEslice(getEra());
    	allocator.allocate(ret, size);
    	return ret;
    }

    public HEslice allocateCAS(int size) {
    	HEslice ret = new HEslice(getEra());
    	allocator.allocate(ret, size);
    	UnsafeUtils.unsafe.fullFence();
    	return ret;
    }

    /**
     * Progress Condition: wait-free bounded (by maxHEs)
     */
     public void clear(int tid) {
    	 UnsafeUtils.unsafe.storeFence(); //need it so that read and writes before doesnt get reordered after
    	 he[(tid)*CLPAD] = NONE;
	 }
     
     
    /**
     * Progress Condition: lock-free
     */
     
     public HEslice get_protected(HEslice obj, int ihe, int tid) {
    	 long prevEra = he[(tid)*CLPAD+ihe];
    	 while (true) {
    		 //T loadedOBJ= obj;//memory_order_seq_cst	in c++ is mapped to 
		    				//A load operation with this memory order performs an acquire operation,
		    				//a store performs a release operation,
		    				//and read-modify-write performs both an acquire operation and a release operation,
		    				//plus a single total order exists in which all threads observe all modifications in the same order
    		 if(obj != null && obj.deadEra != -1)
    				throw new NovaIllegalAccess();

    		 UnsafeUtils.unsafe.loadFence();
    		 long era = eraClock.get();
    		 UnsafeUtils.unsafe.loadFence(); ////must be here this is aquvilent to the acquire 

    		 if (era == prevEra) return obj ;
    		 UnsafeUtils.unsafe.fullFence();
    		 he[tid*CLPAD+ihe] = era; //TODO he must be volatile
    		 prevEra = era;
		}
    }

    /**
     * Retire an object (node)
     * Progress Condition: wait-free bounded
     *
     */
      public <T> void retire(int mytid, HazardEras_interface obj) {
        long currEra = eraClock.get();
        obj.setDeleteEra(currEra);
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*CLPAD];
        rlist.add(obj);
        releasecounter[mytid *2* CLPAD ]++;
        if(releasecounter[mytid *2* CLPAD] == RELEASE_LIST_LIMIT) {
            if (eraClock.get() == currEra) eraClock.getAndAdd(1);
            HazardEras_interface toDeleteObj;
            for (int iret = 0; iret < rlist.size();) {
            	toDeleteObj = (HazardEras_interface)rlist.get(iret);
                if (canDelete(toDeleteObj, mytid)) {
                	rlist.remove(toDeleteObj);
                	allocator.free((NovaSlice)toDeleteObj);
                    continue;
                }
                iret++;
            }
            releasecounter[mytid *2* CLPAD ] = 0;
        }

    }
      
      public <T> void retireCAS(int mytid, HazardEras_interface obj) {
    	  long currEra = eraClock.get();
          obj.setDeleteEra(currEra);
          UnsafeUtils.unsafe.fullFence();
          ArrayList<HazardEras_interface> rlist = retiredList[mytid*CLPAD];
          rlist.add(obj);
          releasecounter[mytid *2* CLPAD ]++;
          if(releasecounter[mytid *2* CLPAD] == RELEASE_LIST_LIMIT) {
              if (eraClock.get() == currEra) eraClock.getAndAdd(1);
              HazardEras_interface toDeleteObj;
              for (int iret = 0; iret < rlist.size();) {
              	toDeleteObj = (HazardEras_interface)rlist.get(iret);
                  if (canDelete(toDeleteObj, mytid)) {
                  	rlist.remove(toDeleteObj);
                  	allocator.free((NovaSlice)toDeleteObj);
                      continue;
                  }
                  iret++;
              }
              releasecounter[mytid *2* CLPAD ] = 0;
          }

      }
      
     
      public void fastFree(NovaSlice s) {
    	  allocator.free(s);
      }

      private boolean  canDelete(HazardEras_interface obj,  int mytid) {
    	  for (int tid = 0; tid < HE_MAX_THREADS; tid++) {
            long era = he[tid*CLPAD ]; //we want to garantue that the last updated he is seen
            UnsafeUtils.unsafe.loadFence();
            if (era == NONE || era < obj.getnewEra() || era > obj.getdelEra()) continue;
            return false;
            }
    	  return true;
    	  }


	//DEBUG
	void ForceCleanUp() {
		for(int i =0 ; i < HE_MAX_THREADS; i++) {
	        ArrayList<HazardEras_interface> rlist = retiredList[i*CLPAD];
            HazardEras_interface toDeleteObj;
	        for (int iret = 0; iret < rlist.size(); ) {
            	toDeleteObj = (HazardEras_interface)rlist.get(iret);
            	allocator.free((NovaSlice)toDeleteObj);
                iret++;
            }

		}
	}

}
