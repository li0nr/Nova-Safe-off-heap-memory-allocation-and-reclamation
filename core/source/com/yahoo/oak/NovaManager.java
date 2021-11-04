/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;


public class NovaManager implements MemoryManager {
    static final int INVALID_SLICE 		= -1;    
    static final int HEADER_SIZE 		= Long.BYTES;
    static final int MAGIC_SIZE 		= Long.BYTES;
    static final long MAGIC_NUM = UUID.randomUUID().getLeastSignificantBits();
    static final int IDENTRY = 0;
    static final int REFENTRY = 1;
    
    private final ArrayList<NovaSlice>[] releaseLists;
    
    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    private final AtomicLongArray TAP;
    private final NovaSlice[] Slices;

    public NovaManager(BlockMemoryAllocator allocator) {
    	
        this.releaseLists = new ArrayList[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
        this.Slices = new NovaSlice[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
        this.TAP = new AtomicLongArray(_Global_Defs.MAX_THREADS * _Global_Defs.CACHE_PADDING * 2);
        this.allocator = allocator;

        for (int i = _Global_Defs.CACHE_PADDING; i < _Global_Defs.MAX_THREADS * _Global_Defs.CACHE_PADDING* 2; i+=_Global_Defs.CACHE_PADDING) {
            this.releaseLists[i]	= new ArrayList<>(_Global_Defs.RELEASE_LIST_LIMIT);
            this.Slices		 [i]	= new NovaSlice(INVALID_SLICE,INVALID_SLICE,INVALID_SLICE);
            this.TAP.set(i+IDENTRY, -1); 
            }     
        globalNovaNumber = new AtomicInteger(1);
    }
    

    @Override
    public void close() {
        allocator.close();
    }

    @Override
    public boolean isClosed() {
        return allocator.isClosed();
    }

    @Override
    public int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    @Override
    public long allocated() {
        return allocator.allocated();
    }

    
    @Override
    public void allocate(NovaSlice s, int size, int idx) {
    	try {
        	boolean allocated = allocator.allocate(s, size+ HEADER_SIZE);
        	assert allocated;
    	}catch(OakOutOfMemoryException e) {
    		InstantTryRelease(idx);
        	boolean allocated = allocator.allocate(s, size+ HEADER_SIZE);
        	assert allocated;
    	}
        s.setHeader(globalNovaNumber.get(),size);
    }
    
    public void allocate_Magic(NovaSlice s, int size) {
        boolean allocated = allocator.allocate(s, size+ HEADER_SIZE + MAGIC_SIZE);
        assert allocated;
        s.setHeader_Magic(globalNovaNumber.get(),size);
    }
    
    public void release(int block, int offset, int len, int idx) {
        List<NovaSlice> myReleaseList = this.releaseLists[(idx+1)*_Global_Defs.CACHE_PADDING];
        myReleaseList.add(new NovaSlice(block,offset,len));
        
        if (myReleaseList.size() >= _Global_Defs.RELEASE_LIST_LIMIT) {
        	
            ArrayList<Long> HostageSlices=new ArrayList<>();
            for (int i = _Global_Defs.CACHE_PADDING; i < 2*_Global_Defs.CACHE_PADDING*_Global_Defs.MAX_THREADS; i= i +_Global_Defs.CACHE_PADDING ) {
        		if(TAP.get(i+IDENTRY) != -1)
        			HostageSlices.add(TAP.get(i+REFENTRY));
        		}
        	globalNovaNumber.incrementAndGet();

        	NovaSlice toDeleteObj;
            for (int iret = 0; iret < myReleaseList.size();) {
            	toDeleteObj = myReleaseList.get(iret);
                if (! HostageSlices.contains(toDeleteObj.getRef())) {
                	myReleaseList.remove(toDeleteObj);
                	allocator.free(toDeleteObj);
                	continue;
                }
                iret++;
            }
        }
    }
    
    public void InstantTryRelease(int idx) {
        List<NovaSlice> myReleaseList = this.releaseLists[(idx+1)*_Global_Defs.CACHE_PADDING];
        ArrayList<Long> HostageSlices=new ArrayList<>();
        for (int i = _Global_Defs.CACHE_PADDING; i < 2*_Global_Defs.CACHE_PADDING*_Global_Defs.MAX_THREADS; i= i +_Global_Defs.CACHE_PADDING ) {
    		if(TAP.get(i+IDENTRY) != -1)
    			HostageSlices.add(TAP.get(i+REFENTRY));
    		}
    	globalNovaNumber.incrementAndGet();
    	int j  =0 ;
    	NovaSlice toDeleteObj;
        for (int iret = 0; iret < myReleaseList.size();) {
        	toDeleteObj = myReleaseList.get(iret);
            if (! HostageSlices.contains(toDeleteObj.getRef())) {
            	myReleaseList.remove(toDeleteObj);
            	allocator.free(toDeleteObj); j++;
            	continue;
            }
            iret++;
        }
        System.out.print("Total freeed is " + j+"\n" );
    }
    
    public boolean free(NovaSlice s) {
    	allocator.free(new NovaSlice(s));
    	return true; //assumes always successful!
    }


  public  void setTap(long ref,int idx) {
	int i= idx%_Global_Defs.MAX_THREADS;
	TAP.set(_Global_Defs.CACHE_PADDING*(i+1)+IDENTRY, idx);
	TAP.set(_Global_Defs.CACHE_PADDING*(i+1)+REFENTRY, ref);
  }
    
  public  void UnsetTap(int idx) {
	int i= idx%_Global_Defs.MAX_THREADS;
	TAP.set(_Global_Defs.CACHE_PADDING*(i+1)+IDENTRY, -1);
  }

    public long getAdress(int blockID) {
    	return allocator.getAddress(blockID);
    }


    public NovaSlice getSlice(int size,int ThreadIdx) {
    	NovaSlice s = Slices[(ThreadIdx+1)*_Global_Defs.CACHE_PADDING];
    	allocate(s, size,ThreadIdx);
    	return s;
    
    }
    
    public NovaSlice getSlice_Magic(int size,int ThreadIdx) {
    	NovaSlice s = Slices[(ThreadIdx+1)*_Global_Defs.CACHE_PADDING];
    	allocate_Magic(s, size);
    	return s;
    
    }
    public NovaSlice privateSlice(int ThreadIdx) {
    	return Slices[(ThreadIdx+1)*_Global_Defs.CACHE_PADDING];
    }
    public int  getNovaEra() {
        return globalNovaNumber.get();
    }
    
    public void ForceCleanUp() {//UNSAFE reclimation for deubg
//    	for(List<NovaSlice> a : releaseLists) {
//    		for(NovaSlice x : a) {
//    			allocator.free(x);
//    		}
//    	}
    	for(int i= _Global_Defs.CACHE_PADDING; i < 2*_Global_Defs.CACHE_PADDING*_Global_Defs.MAX_THREADS; i +=_Global_Defs.CACHE_PADDING ) {
    		for(NovaSlice x : releaseLists[i]) {
    			allocator.free(x);
    		}
    	}
    }
        
}
