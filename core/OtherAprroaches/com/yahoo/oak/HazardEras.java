package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import sun.misc.Unsafe;

public class HazardEras{

	private static final long NONE = 0;

    private int[] releasecounter;

    private final AtomicLong eraClock;
    private AtomicLongArray he;
    private final ArrayList<HazardEras_interface>[] retiredList;
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
		 
	public HazardEras(NativeMemoryAllocator alloc) {
		allocator = alloc;
		eraClock 		= new AtomicLong(NONE);
		he 				= new AtomicLongArray(_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING);
		releasecounter 	= new int[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
		retiredList		= new ArrayList[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
		 
    	for(int it=0; it< _Global_Defs.MAX_THREADS; it++) {
			he.set(it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING ,NONE);
    		retiredList[it*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING ] = new ArrayList<HazardEras_interface>();
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


    /**
     * Progress Condition: wait-free bounded (by maxHEs)
     */
     public void clear(int tid) {
    	 UnsafeUtils.unsafe.storeFence(); //need it so that read and writes before doesnt get reordered after
    	 he.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING,NONE);
	 }
     
     
    /**
     * Progress Condition: lock-free
     */
     
     public HEslice get_protected(HEslice obj, int tid) {
    	 long prevEra = he.get(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING);
    	 while (true) {
    		 				//T loadedOBJ= obj;//memory_order_seq_cst	in c++ is mapped to 
		    				//A load operation with this memory order performs an acquire operation,
		    				//a store performs a release operation,
		    				//and read-modify-write performs both an acquire operation and a release operation,
		    				//plus a single total order exists in which all threads observe all modifications in the same order
    		 if(obj != null && obj.deadEra != -1) //TODO I think deadEra should be volatile
    			 return null;					  // if something was deleted but the dead still did not get updated 

    		 //UnsafeUtils.unsafe.loadFence();
    		 //this fence is the equivalence for atom.load() in cpp
    		 long era = eraClock.get();			
    		 									
    		 UnsafeUtils.unsafe.loadFence(); 	
    		 //this is needed scenario if obj is has been deleted for a while 
    		 //we enter get protected without read barrier we can read era and exit with obj before checking if it is deleted
    		 //this fence is the equivalence for eraClock.load(memory_order_acquire);
    		 if (era == prevEra) return obj ;
    		 UnsafeUtils.unsafe.fullFence();
    		 he.set(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING, era); //TODO he must be volatile
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
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
        rlist.add(obj);
        releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]++;
        if(releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] == _Global_Defs.RELEASE_LIST_LIMIT) {
            if (eraClock.get() == currEra) eraClock.getAndAdd(1);
            HazardEras_interface toDeleteObj;
            for (int iret = 0; iret < rlist.size();) {
            	toDeleteObj = (HazardEras_interface)rlist.get(iret);
                if (canDelete(toDeleteObj, mytid)) {
                	rlist.remove(toDeleteObj);
                	allocator.free(new NovaSlice((NovaSlice)toDeleteObj));
                    continue;
                }
                iret++;
            }
            releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] = 0;
        }

    }
      

      private boolean  canDelete(HazardEras_interface obj,  int mytid) {
    	  for (int tid = 0; tid < _Global_Defs.MAX_THREADS; tid++) {
            long era = he.get(tid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING); //we want to garantue that the last updated he is seen
            UnsafeUtils.unsafe.loadFence();
            if (era == NONE || era < obj.getnewEra() || era > obj.getdelEra()) continue;
            return false;
            }
    	  return true;
    	  }

      public void fastFree(NovaSlice s) {
    	  allocator.free(s);
      }

	//DEBUG
	void ForceCleanUp() {
		for(int i =0 ; i < _Global_Defs.MAX_THREADS; i++) {
	        ArrayList<HazardEras_interface> rlist = retiredList[i*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
            HazardEras_interface toDeleteObj;
	        for (int iret = 0; iret < rlist.size(); ) {
            	toDeleteObj = (HazardEras_interface)rlist.get(iret);
            	allocator.free((NovaSlice)toDeleteObj);
                iret++;
            }

		}
	}
		
	//Adding the CAS 
    public HEslice allocateCAS(int size) {
    	HEslice ret = new HEslice(getEra());
    	allocator.allocate(ret, size);
    	UnsafeUtils.unsafe.storeFence();
    	return ret;
    }
    
    public <T> void retireCAS(int mytid, HazardEras_interface obj) {
  	  long currEra = eraClock.get();
        obj.setDeleteEra(currEra);
        UnsafeUtils.unsafe.storeFence();
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING];
        rlist.add(obj);
        releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING]++;
        if(releasecounter[mytid *_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] == _Global_Defs.RELEASE_LIST_LIMIT) {
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
            releasecounter[mytid*_Global_Defs.CACHE_PADDING+_Global_Defs.CACHE_PADDING] = 0;
        }

    }
}
