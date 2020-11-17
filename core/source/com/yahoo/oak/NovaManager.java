/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    
    static final int IDENTRY = 8;
    static final int REFENTRY = 9;
    private final ThreadIndexCalculator threadIndexCalculator;
    private final List<List<Slice>> releaseLists;
    private final List<List<NovaSlice>> NreleaseLists;

    private final AtomicInteger globalNovaNumber;
    public final BlockMemoryAllocator allocator;
    
    private final int blockcount;
    //private final CopyOnWriteArrayList<Long> TAP;
    
    private final long TAP[][][];
    private final List<NovaReadBuffer> ReadBuffers;
    private final List<NovaWriteBuffer> WriteBuffers;
    private final List<NovaSlice> Slices;

    NovaManager(BlockMemoryAllocator allocator) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        this.NreleaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.NreleaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        //initialized once to be always used!
        NovaSlice s=new NovaSlice(0,-1,0);
        this.ReadBuffers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.ReadBuffers.add(new NovaReadBuffer(s));
        }
        this.WriteBuffers = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.WriteBuffers.add(new NovaWriteBuffer(s));
        }
        this.Slices = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.Slices.add(new NovaSlice(0,-1,0));
        }
        blockcount = allocator.getBlocks();
//        TAP = new TAP_entry[blockcount][ThreadIndexCalculator.MAX_THREADS];
//        for(int j=0; j<blockcount ; j++) {
//            for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
//        		this.TAP[j][i] = new TAP_entry();
//        	}
//        }
      TAP = new long[blockcount][ThreadIndexCalculator.MAX_THREADS][16];
      for(int j=0; j<blockcount ; j++) {
          for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
      		this.TAP[j][i][8] =-1;
      		this.TAP[j][i][9] =-1;

      	}
      }

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
        int idx = threadIndexCalculator.getIndex();
        List<Slice> myReleaseList = this.releaseLists.get(idx);
        myReleaseList.add(new Slice(s));
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalNovaNumber.incrementAndGet();
            for (Slice allocToRelease : myReleaseList) {
                allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
    }
    
    public void release(NovaSlice s) {
        int idx = threadIndexCalculator.getIndex();
        List<NovaSlice> myReleaseList = this.NreleaseLists.get(idx);
        myReleaseList.add(new NovaSlice(s));
        if (myReleaseList.size() >= 1) {
            globalNovaNumber.incrementAndGet();
            for (NovaSlice allocToRelease : myReleaseList) {
            	if(!containsRef(allocToRelease.getAllocatedBlockID() ,allocToRelease.getRef()))
            			allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
    }

//    public  void setTaps(int block,long ref) {
//    	long index = Thread.currentThread().getId();
//    	TAPS.putIfAbsent(block, new TAP_entry[32]())
//    	for(int i=(int)index%threadIndexCalculator.MAX_THREADS ; i<ThreadIndexCalculator.MAX_THREADS; i++) {
//    		if(TAP[i].id == -1){
//    			TAP[i].ref = ref;
//    			TAP[i].id = index;
//    			return;
//    		}
//    	}throw new IllegalAccessError("FAILED SETTING TAP");
//    }
//    public  void setTap(int block,long ref) {
//    	long index = Thread.currentThread().getId();
//    	int i= (int)index%32;
//    	//for(int i=(int)index%threadIndexCalculator.MAX_THREADS ; i<ThreadIndexCalculator.MAX_THREADS; i++) {
//    		if(TAP[block][i].id == -1){
//    			TAP[block][i].ref = ref;
//    			TAP[block][i].id = index;
//    			return;
//    		}
//    		else throw new IllegalAccessError("FAILED SETTING TAP");
//    }

//    public  void UnsetTap(int block,long ref) {
//    	long index = Thread.currentThread().getId();
//    	int i= (int)index%32;
//    		if(TAP[block][i].id == index){
//    			TAP[block][i].id = -1;
//    			return;
//    		}
//    		else throw new IllegalAccessError("FAILED SETTING TAP");
//	}
    
  public  void setTap(int block,long ref) {
	long index = Thread.currentThread().getId();
	int i= (int)index%32;
	//for(int i=(int)index%threadIndexCalculator.MAX_THREADS ; i<ThreadIndexCalculator.MAX_THREADS; i++) {
		if(TAP[block][i][IDENTRY]== -1){
			TAP[block][i][REFENTRY]= ref;
			TAP[block][i][IDENTRY] = index;
			return;
		}
		else throw new IllegalAccessError("FAILED SETTING TAP");
}
    
  public  void UnsetTap(int block,long ref) {
	long index = Thread.currentThread().getId();
	int i= (int)index%32;
		if(TAP[block][i][IDENTRY] == index){
			TAP[block][i][IDENTRY] = -1;
			return;
		}
		else throw new IllegalAccessError("FAILED SETTING TAP");
}

    @Override
    public void readByteBuffer(Slice s) {
        allocator.readByteBuffer(s);
    }
    
    @Override 
    public void readByteBuffer(NovaSlice s) {
        allocator.readByteBuffer(s);
    }
    
    public void readByteBuffer(Facade f,int block) {
        allocator.readByteBuffer(f,block);
    }


    public ByteBuffer readByteBuffer(int block) {
        return allocator.readByteBuffer(block);
    }
    
    public NovaReadBuffer getReadBuffer(NovaSlice s) {
    	int idx = threadIndexCalculator.getIndex();
    	NovaReadBuffer buff= ReadBuffers.get(idx);
    	buff.adjustSlice(s,this);
    	return buff;
    
    }
    
    public NovaWriteBuffer getWriteBuffer(NovaSlice s) {
    	int idx = threadIndexCalculator.getIndex();
    	NovaWriteBuffer buff= WriteBuffers.get(idx);
    	buff.adjustSlice(s,this);
    	return buff;
    
    }
    
    public NovaSlice getSlice(int size) {
    	int idx = threadIndexCalculator.getIndex();
    	NovaSlice s=Slices.get(idx);
    	allocate(s, size);
    	return s;
    
    }
    public int  getNovaEra() {
        return globalNovaNumber.get();
    }
    
//    @Contended
//    private class TAP_entry {
//         public long ref;
//         public long id;
//        
//    TAP_entry(){
//    	id = -1;
//    }

//    }
    private
    boolean containsRef(int block,long ref) {
    	for( long[] entry: TAP[block]) {
    		if(entry[IDENTRY]!=-1 && entry[REFENTRY] == ref) return true;
    	}
    	return false;
    }
    
}
