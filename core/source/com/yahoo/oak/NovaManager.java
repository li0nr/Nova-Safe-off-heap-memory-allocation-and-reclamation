/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;


class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    static final int MAX_THREADS = 32;
    static final int INVALID_SLICE = -1;
    
    static final int IDENTRY = 0;
    static final int REFENTRY = 1;
    static final int CACHE_PADDING = 16;
    static final int BLOCK_TAP = CACHE_PADDING*MAX_THREADS;
    
    private final List<List<Slice>> OreleaseLists;
    private final List<List<NovaSlice>> NreleaseLists;

    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    
    private final int blockcount;    
    private final long TAP[];

    private final List<NovaReadBuffer> ReadBuffers;
    private final List<NovaWriteBuffer> WriteBuffers;
    private final List<NovaSlice> Slices;

    NovaManager(BlockMemoryAllocator allocator) {
        this.OreleaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.OreleaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        this.NreleaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.NreleaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        //initialized once to be always used!
        /***************************************************/
        NovaSlice s=new NovaSlice(0,-1,0);
        this.ReadBuffers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.ReadBuffers.add(new NovaReadBuffer(s));
        }
        this.WriteBuffers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.WriteBuffers.add(new NovaWriteBuffer(s));
        }
        /***************************************************/
        this.Slices = new ArrayList<>();
        for (int i = 0; i < MAX_THREADS; i++) {
            this.Slices.add(new NovaSlice(INVALID_SLICE,INVALID_SLICE,INVALID_SLICE));
        }
        blockcount = allocator.getBlocks();
        
        
        TAP = new long[blockcount * MAX_THREADS*CACHE_PADDING];
        for(int i=BLOCK_TAP; i<blockcount*MAX_THREADS*CACHE_PADDING; i+=CACHE_PADDING)
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
    public void allocate(Slice s, int size, Allocate allocate) {
        boolean allocated = allocator.allocate(s, size, allocate);
        assert allocated;
        s.setVersion(globalNovaNumber.get());
    }
    
    @Override
    public void allocate(NovaSlice s, int size) {
        boolean allocated = allocator.allocate(s, size);
        assert allocated;
        s.setHeader(globalNovaNumber.get(),size);
    }

    @Override
    public void release(Slice s) {
        int idx = 0;
        List<Slice> myReleaseList = this.OreleaseLists.get(idx);
        myReleaseList.add(new Slice(s));
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalNovaNumber.incrementAndGet();
            for (Slice allocToRelease : myReleaseList) {
                allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
    }
    
    public void release(int block, int offset, int len, int idx) {
    	
        List<NovaSlice> myReleaseList = this.NreleaseLists.get(idx);
        myReleaseList.add(new NovaSlice(block,offset,len));
        
        if (myReleaseList.size() >= 1) {
        	
            ArrayList<Long> releasedSlices=new ArrayList<>();
        	for(int i=block*BLOCK_TAP; i<block*BLOCK_TAP+BLOCK_TAP; i+=CACHE_PADDING) {
        		if(TAP[i+IDENTRY] != -1)
        			releasedSlices.add(TAP[i+REFENTRY]);
        	}
        	globalNovaNumber.incrementAndGet();
        	Iterator<NovaSlice> itr=myReleaseList.iterator();
        	while(itr.hasNext()) {
        		NovaSlice tmp=itr.next();
        		if(!releasedSlices.contains(tmp.getRef())) {
        			allocator.free(tmp);
        			itr.remove();
        		}
        	}
//            myReleaseList.forEach(s -> {if(!containsRef(s.getAllocatedBlockID(),s.getRef()))
//            								allocator.free(s);
//            							});
//            myReleaseList.removeIf(s->!containsRef(s.getAllocatedBlockID(),s.getRef()));
        }
    }
    
    public boolean free(NovaSlice s) {
    	allocator.free(s);
    	return true; //assumes always successful!
    }


  public  void setTap(int block,long ref,int idx) {
	int i= idx%MAX_THREADS;
	TAP[block*BLOCK_TAP+CACHE_PADDING*i+IDENTRY]=idx;
	TAP[block*BLOCK_TAP+CACHE_PADDING*i+REFENTRY]=ref;
}
    
  public  void UnsetTap(int block,int idx) {
	int i= idx%MAX_THREADS;
	TAP[block*BLOCK_TAP+CACHE_PADDING*i+IDENTRY]=-1;
}

    @Override
    public void readByteBuffer(Slice s) {
        allocator.readByteBuffer(s);
    }
    
    @Override 
    public void readByteBuffer(NovaSlice s) {
        allocator.readByteBuffer(s);
    }


    public ByteBuffer readByteBuffer(int block) {
        return allocator.readByteBuffer(block);
    }
    
    /***************************************************/
//    public NovaReadBuffer getReadBuffer(NovaSlice s) {
//    	int idx = threadIndexCalculator.getIndex();
//    	NovaReadBuffer buff= ReadBuffers.get(idx);
//    	buff.adjustSlice(s,this);
//    	return buff;
//    
//    }
//    
//    public NovaWriteBuffer getWriteBuffer(NovaSlice s) {
//    	int idx = threadIndexCalculator.getIndex();
//    	NovaWriteBuffer buff= WriteBuffers.get(idx);
//    	buff.adjustSlice(s,this);
//    	return buff;
//    
//    }
    /***************************************************/

    
    public NovaSlice getSlice(int size,int ThreadIdx) {
    	NovaSlice s=Slices.get(ThreadIdx);
    	allocate(s, size);
    	return s;
    
    }
  

    public int  getNovaEra() {
        return globalNovaNumber.get();
    }
    
    private  boolean containsRef(int block,long ref) {
    	for(int i=block*BLOCK_TAP; i<block*BLOCK_TAP+BLOCK_TAP; i+=CACHE_PADDING) {
    		if(TAP[i+IDENTRY]!=-1 && TAP[i+REFENTRY] == ref) return true;
    	}
    	return false;
    }
    
}
