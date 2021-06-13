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
    static final int CACHE_PADDING = 16;
    static final int BLOCK_TAP = CACHE_PADDING*MAX_THREADS;
    
    private final List<List<NovaSlice>> NreleaseLists;

    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    
    private final int blockcount;    
    private final long TAP[];

//    private final List<NovaReadBuffer> ReadBuffers;
//    private final List<NovaWriteBuffer> WriteBuffers;
    private final List<NovaSlice> Slices;

    public NovaManager(BlockMemoryAllocator allocator) {
        this.NreleaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.NreleaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        this.Slices = new ArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.Slices.add(new NovaSlice(INVALID_SLICE,INVALID_SLICE,INVALID_SLICE));
        }
        blockcount = allocator.getBlocks();
        
        TAP = new long[ MAX_THREADS * CACHE_PADDING];
        //block 0 is not used ?
        for(int i=0 ; i < MAX_THREADS*CACHE_PADDING; i+=CACHE_PADDING)
        	TAP[i+IDENTRY]=-1;

        
        globalNovaNumber = new AtomicInteger(1);
        this.allocator = allocator;
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
        List<NovaSlice> myReleaseList = this.NreleaseLists.get(idx);
        myReleaseList.add(new NovaSlice(block,offset,len));
        
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
        	
            ArrayList<Long> HostageSlices=new ArrayList<>();
            for (int i = 0; i < CACHE_PADDING*MAX_THREADS; i= i +CACHE_PADDING ) {
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
	TAP[CACHE_PADDING*i+IDENTRY]=idx;
	TAP[CACHE_PADDING*i+REFENTRY]=ref;
}
    
  public  void UnsetTap(int block,int idx) {
	int i= idx%MAX_THREADS;
	TAP[CACHE_PADDING*i+IDENTRY]=-1;
}

    public long getAdress(int blockID) {
    	return allocator.getAddress(blockID);
    }


    
    public NovaSlice getSlice(int size,int ThreadIdx) {
    	NovaSlice s=Slices.get(ThreadIdx);
    	allocate(s, size);
    	return s;
    
    }
  

    public int  getNovaEra() {
        return globalNovaNumber.get();
    }
    
    public void ForceCleanUp() {//UNSAFE reclimation for deubg
    	for(List<NovaSlice> a : NreleaseLists) {
    		for(NovaSlice x : a) {
    			allocator.free(x);
    		}
    	}
    }
    
}
