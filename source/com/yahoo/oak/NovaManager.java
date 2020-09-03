/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


class NovaManager implements MemoryManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private final ThreadIndexCalculator threadIndexCalculator;
    private final List<List<Slice>> releaseLists;
    private final List<List<NovaSlice>> NreleaseLists;

    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    
    private final List<Long> TAP;
    
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
        this.TAP = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.TAP.add(new Long(0));
        }
        //initialized once to be always used!
        NovaSlice s=new NovaSlice(0,-1);
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
            this.Slices.add(new NovaSlice(0,-1));
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
            	if(!TAP.contains(allocToRelease.getRef()))
            			allocator.free(allocToRelease);
            }
            myReleaseList.clear();
        }
    }

    public  boolean setTap(long ref) {
        int idx = threadIndexCalculator.getIndex();
        Long O=this.TAP.set(idx, ref);
        if(O.equals((long)0)) return true;
        else return false; 
    }

    public  boolean UnsetTap(long ref) {
    	if (!this.TAP.contains(ref)) 
    		return true;
        int idx = threadIndexCalculator.getIndex();
        Long O=this.TAP.set(idx, (long)0);
        if(O.equals(ref)) return true;
        else return false; 
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
}
