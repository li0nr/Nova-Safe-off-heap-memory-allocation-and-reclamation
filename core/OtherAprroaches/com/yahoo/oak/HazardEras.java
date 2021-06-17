package com.yahoo.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.Unsafe;

public class HazardEras {

/*
 * <h1> Hazard Eras </h1>
 * This a light-weight implementation of hazard eras, where each thread has a
 * thread-local list of retired objects.
 *
 * This is based on the paper "Hazard Eras - Non-Blocking Memory Reclamation"
 * by Pedro Ramalhete and Andreia Correia:
 * https://github.com/pramalhe/ConcurrencyFreaks/blob/master/papers/hazarderas-2017.pdf
 *
 * The type T is for the objects/nodes and it's it must have the members newEra, delEra
 *
 * R is zero.
 *
 * <p>
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */

	private static final long NONE = -1;
	private static final int  HE_MAX_THREADS = 32;
	private static final int CLPAD = 33;
	private static final int  HE_THRESHOLD_R = 0; 
	private static final int RELEASE_LIST_LIMIT = 1024;

	private static int  MAX_HES = 5; 


    private final int             maxThreads= 32;
    private int[] releasecounter = new int[maxThreads*CLPAD];

    private final AtomicLong eraClock;
    private long[] he = new long[HE_MAX_THREADS*MAX_HES*CLPAD];
    private final ArrayList<HazardEras_interface>[] retiredList= new ArrayList[HE_MAX_THREADS*CLPAD];//CLPAD is for cache padding
    private final NativeMemoryAllocator allocator;
    
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	
	
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
		 
	public HazardEras(int maxHEs, int maxThreads, NativeMemoryAllocator alloc) {
		allocator = alloc;
		MAX_HES = maxHEs;
		eraClock = new AtomicLong(1);
		he = new long[HE_MAX_THREADS*MAX_HES*CLPAD];
    	for(int it=0; it< HE_MAX_THREADS; it++) {
    		for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
    			he[it*CLPAD + 16 + ihe]= (1);
    		}
    		retiredList[it*CLPAD+16] = new ArrayList<HazardEras_interface>();
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
    	 for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
    		 UnsafeUtils.unsafe.fullFence();
    			he[(tid)*CLPAD+16+ihe] = 0;
    			}
    	 }
     
     

    /**
     * Progress Condition: lock-free
     */
     
     public HEslice get_protected(HEslice obj, int ihe, int tid) {
    	 long prevEra = he[(tid)*CLPAD+16+ihe];
    	 while (true) {
    		 //T loadedOBJ= obj;//memory_order_seq_cst	in c++ is mapped to 
		    				//A load operation with this memory order performs an acquire operation,
		    				//a store performs a release operation,
		    				//and read-modify-write performs both an acquire operation and a release operation,
		    				//plus a single total order exists in which all threads observe all modifications in the same order
    		 if(obj != null && obj.deadEra != -1)
    				throw new IllegalArgumentException("slice was deleted");

    		 UNSAFE.loadFence(); 
    		 long era = eraClock.get();
    		 UNSAFE.loadFence(); ////must be here this is aquvilent to the acquire 

    		 if (era == prevEra) return obj ;
    		 UNSAFE.fullFence(); 
    		 he[tid*CLPAD+16+ihe] = era; //TODO he must be volatile
    		 prevEra = era;
		}
    }


     void protectEraRelease(int index, int other, int tid) {
         long era = he[(tid)*CLPAD+16+other];
         if (he[(tid)*CLPAD+16+index] == era) return;
         he[(tid)*CLPAD+16+index] = era;
		 UNSAFE.fullFence(); 
     }
     
    /**
     * Retire an object (node)
     * Progress Condition: wait-free bounded
     *
     */
      public <T> void retire(int mytid, HazardEras_interface obj) {
        long currEra = eraClock.get();
//        ptr->delEra = currEra;
        obj.setDeleteEra(currEra);
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*CLPAD+ 16];
        rlist.add(obj);
        releasecounter[mytid *CLPAD + 16]++;
        if(releasecounter[mytid *CLPAD + 16] == RELEASE_LIST_LIMIT) {
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
            releasecounter[mytid *CLPAD+ 16]=0;
        }

    }
      
      public void fastFree(NovaSlice s) {
    	  allocator.free(s);
      }

private    boolean  canDelete(HazardEras_interface obj,  int mytid) {
        for (int tid = 0; tid < maxThreads; tid++) {
            for (int ihe = 0; ihe < MAX_HES; ihe++) {
                long era = he[tid*CLPAD + 16 + ihe]; //we want to garantue that the last updated he is seen
                UNSAFE.loadFence();
                if (era == NONE || era < obj.getnewEra() || era > obj.getdelEra()) continue;
                return false;
            }
        }
        return true;
    }


	//DEBUG
	void ForceCleanUp() {
		for(int i =0 ; i < HE_MAX_THREADS; i++) {
	        ArrayList<HazardEras_interface> rlist = retiredList[i*CLPAD+ 16];
            HazardEras_interface toDeleteObj;
	        for (int iret = 0; iret < rlist.size(); ) {
            	toDeleteObj = (HazardEras_interface)rlist.get(iret);
            	allocator.free((NovaSlice)toDeleteObj);
                iret++;
            }

		}
	}

}
