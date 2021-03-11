package com.yahoo.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.Unsafe;

public class _HazardEras_ {

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
	private static final int  MAX_HES = 5; 
	private static final int CLPAD = 16;
	private static final int  HE_THRESHOLD_R = 0; 


    private final int             maxHEs = 2;
    private final int             maxThreads= 32;

    private final AtomicLong eraClock;
    private AtomicLong[] he = new AtomicLong[HE_MAX_THREADS*MAX_HES*CLPAD];
    private final ArrayList<_HazardEras_interface>[] retiredList= new ArrayList[HE_MAX_THREADS*4];//4 is for cache padding
    
    
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	
	_HazardEras_(int maxHEs, int maxThreads) {
		eraClock = new AtomicLong(1);
    	for(int it=0; it< HE_MAX_THREADS; it++) {
            //he[it] = new std::atomic<uint64_t>[CLPAD*2]; // We allocate four cache lines to allow for many hps and without false sharing
    		//retiredList[it*CLPAD].reserve(maxThreads*maxHEs); java deals with this 
    		for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
    			he[(it+ihe)*CLPAD]= new AtomicLong(NONE);
    		}
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
     	for(int it=0; it< HE_MAX_THREADS; it++) {
     		for( int ihe= 0; ihe < MAX_HES ; ihe ++) {
			he[(it+ihe)*CLPAD]= new AtomicLong(NONE);
			}
     	}
    }

    /**
     * Progress Condition: lock-free
     */
     <T> T get_protected(T obj, int index, int tid) {
    	 long prevEra = he[(tid+index)*CLPAD].get();
		while (true) {
		   // T* ptr = atom.load();
		    long era = eraClock.get();
			UNSAFE.loadFence();

		    if (era == prevEra) return obj ;
		    he[(tid+index)*CLPAD].set(era);
            prevEra = era;
		}
    }

     
    /**
     * Retire an object (node)
     * Progress Condition: wait-free bounded
     *
     */
      <T> void retire(int mytid, _HazardEras_interface obj) {
        long currEra = eraClock.get();
//        ptr->delEra = currEra;
        obj.setDeleteEra(currEra);
        ArrayList<_HazardEras_interface> rlist = retiredList[mytid*CLPAD];
        rlist.add(obj);
        if (eraClock.get() == currEra) eraClock.getAndAdd(1);
        _HazardEras_interface toDeleteObj;
        for (int iret = 0; iret < rlist.size();) {
        	toDeleteObj = (_HazardEras_interface)rlist.get(iret);
            if (canDelete(toDeleteObj, mytid)) {
            	rlist.remove(toDeleteObj);
             //   delete obj;
                continue;
            }
            iret++;
        }
    }

private    boolean  canDelete(_HazardEras_interface obj,  int mytid) {
        for (int tid = 0; tid < maxThreads; tid++) {
            for (int ihe = 0; ihe < maxHEs; ihe++) {
                long era = (long)he[(tid+ihe)*CLPAD].get();
                UNSAFE.loadFence();
                if (era == NONE || era < obj.getnewEra() || era > obj.getdelEra()) continue;
                return false;
            }
        }
        return true;
    }

}