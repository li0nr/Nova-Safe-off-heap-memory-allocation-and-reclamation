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

	private static final long NONE = 0;
	private static final int  HE_MAX_THREADS = 32;
	private static final int CLPAD = 16;
	private static final int  HE_THRESHOLD_R = 0; 
	private static final int RELEASE_LIST_LIMIT = 1024;

	private static int  MAX_HES = 5; 


    private final int             maxThreads= 32;
    private int[] releasecounter = new int[maxThreads*CLPAD];

    private final AtomicLong eraClock;
    private AtomicLong[] he = new AtomicLong[HE_MAX_THREADS*MAX_HES*CLPAD];
    private final ArrayList<HazardEras_interface>[] retiredList= new ArrayList[HE_MAX_THREADS*CLPAD];//CLPAD is for cache padding
    private final NativeMemoryAllocator allocator;
    
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	
	HazardEras(int maxHEs, int maxThreads, NativeMemoryAllocator alloc) {
		allocator = alloc;
		MAX_HES = maxHEs;
		eraClock = new AtomicLong(1);
    	for(int it=0; it< HE_MAX_THREADS; it++) {
            //he[it] = new std::atomic<uint64_t>[CLPAD*2]; // We allocate four cache lines to allow for many hps and without false sharing
    		//retiredList[it*CLPAD].reserve(maxThreads*maxHEs); java deals with this 
    		for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
    			he[(it+ihe)*CLPAD]= new AtomicLong(NONE);
    		}
    		retiredList[it*CLPAD] = new ArrayList<HazardEras_interface>();
           // static_assert(std::is_same<decltype(T::newEra), uint64_t>::value, "T::newEra must be uint64_t");
           // static_assert(std::is_same<decltype(T::delEra), uint64_t>::value, "T::delEra must be uint64_t");
    	}
    }

    long getEra() {
        return eraClock.get();
        }


    /**
     * Progress Condition: wait-free bounded (by maxHEs)
     */
     void clear(int tid) {
    	 for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
    		 he[(tid+ihe)*CLPAD]= new AtomicLong(NONE);
    		 }
    	 }
     
     

    /**
     * Progress Condition: lock-free
     */
     <T> T get_protected(T obj, int index, int tid) {
    	 long prevEra = he[(tid+index)*CLPAD].get();
		while (true) {
		    T loadedOBJ= obj;
			UNSAFE.loadFence();
		    long era = eraClock.get();
			UNSAFE.loadFence();

		    if (era == prevEra) return loadedOBJ ;
		    he[(tid+index)*CLPAD].set(era);
		    UNSAFE.storeFence();
            prevEra = era;
		}
    }

     
    /**
     * Retire an object (node)
     * Progress Condition: wait-free bounded
     *
     */
      <T> void retire(int mytid, HazardEras_interface obj) {
        long currEra = eraClock.get();
//        ptr->delEra = currEra;
        obj.setDeleteEra(currEra);
        ArrayList<HazardEras_interface> rlist = retiredList[mytid*CLPAD];
        rlist.add(obj);
        if (eraClock.get() == currEra) eraClock.getAndAdd(1);
        releasecounter[mytid *CLPAD]++;
        if(releasecounter[mytid *CLPAD] == RELEASE_LIST_LIMIT) {
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
            releasecounter[mytid *CLPAD]=0;
        }

    }

private    boolean  canDelete(HazardEras_interface obj,  int mytid) {
        for (int tid = 0; tid < maxThreads; tid++) {
            for (int ihe = 0; ihe < MAX_HES; ihe++) {
                long era = (long)he[(tid+ihe)*CLPAD].get();
                UNSAFE.loadFence();
                if (era == NONE || era < obj.getnewEra() || era > obj.getdelEra()) continue;
                return false;
            }
        }
        return true;
    }

}