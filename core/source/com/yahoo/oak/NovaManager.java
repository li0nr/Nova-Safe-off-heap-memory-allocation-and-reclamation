/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


public class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    static final int MAX_THREADS = 32;
    static final int INVALID_SLICE = -1;
    
    static final int HEADER_SIZE = Long.BYTES;
    static final int IDENTRY = 0;
    static final int REFENTRY = 1;
    static final int CACHE_PADDING = 8;
    
    private final ArrayList<NovaSlice>[] releaseLists;
    
    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    private final long TAP[];
    private final NovaSlice[] Slices;

    public NovaManager(BlockMemoryAllocator allocator) {
    	
        this.releaseLists = new ArrayList[MAX_THREADS*2*CACHE_PADDING];
        this.Slices = new NovaSlice[MAX_THREADS*2*CACHE_PADDING];
        this.TAP = new long[ MAX_THREADS * CACHE_PADDING * 2];
        this.allocator = allocator;

        for (int i = CACHE_PADDING; i < MAX_THREADS * CACHE_PADDING* 2; i+=CACHE_PADDING) {
            this.releaseLists[i]	= new ArrayList<>(RELEASE_LIST_LIMIT);
            this.Slices		 [i]	= new NovaSlice(INVALID_SLICE,INVALID_SLICE,INVALID_SLICE);
            this.TAP		 [i+IDENTRY]	= -1;
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
    public void allocate(NovaSlice s, int size) {
        boolean allocated = allocator.allocate(s, size+ HEADER_SIZE);
        assert allocated;
        s.setHeader(globalNovaNumber.get(),size);
    }
    
    public void release(int block, int offset, int len, int idx) {
        List<NovaSlice> myReleaseList = this.releaseLists[(idx+1)*CACHE_PADDING];
        myReleaseList.add(new NovaSlice(block,offset,len));
        
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
        	
            ArrayList<Long> HostageSlices=new ArrayList<>();
            for (int i = CACHE_PADDING; i < 2*CACHE_PADDING*MAX_THREADS; i= i +CACHE_PADDING ) {
        		if(TAP[i+IDENTRY] != -1)
        			HostageSlices.add(TAP[i+REFENTRY]);
        		}
        	globalNovaNumber.incrementAndGet();
        	Iterator<NovaSlice> itr=myReleaseList.iterator();
        	while(itr.hasNext()) {
        		NovaSlice tmp=itr.next();
        		if(!HostageSlices.contains(tmp.getRef())) {
        			allocator.free(tmp);
        			itr.remove();
        		}
        	}
        }
    }
    
    public boolean free(NovaSlice s) {
    	allocator.free(s);
    	return true; //assumes always successful!
    }


  public  void setTap(int block,long ref,int idx) {
	int i= idx%MAX_THREADS;
	TAP[CACHE_PADDING*(i+1)+IDENTRY]=idx;
	TAP[CACHE_PADDING*(i+1)+REFENTRY]=ref;
  }
    
  public  void UnsetTap(int block,int idx) {
	int i= idx%MAX_THREADS;
	TAP[CACHE_PADDING*(i+1)+IDENTRY]=-1;
  }

    public long getAdress(int blockID) {
    	return allocator.getAddress(blockID);
    }


    public NovaSlice getSlice(int size,int ThreadIdx) {
    	NovaSlice s = Slices[(ThreadIdx+1)*CACHE_PADDING];
    	allocate(s, size);
    	return s;
    
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
    	for(int i= CACHE_PADDING; i < 2*CACHE_PADDING*MAX_THREADS; i +=CACHE_PADDING ) {
    		for(NovaSlice x : releaseLists[i]) {
    			allocator.free(x);
    		}
    	}
    }
    
}
