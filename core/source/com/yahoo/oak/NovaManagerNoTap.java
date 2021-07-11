/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;


public class NovaManagerNoTap implements MemoryManager {
    static final int INVALID_SLICE = -1;    
    static final int HEADER_SIZE = Long.BYTES;
    static final int IDENTRY = 0;
    static final int REFENTRY = 1;
    
    private final ArrayList<DeletedSlice>[] releaseLists;
    
    private final AtomicInteger globalNovaNumber;
    private final BlockMemoryAllocator allocator;
    private final AtomicLongArray EpochFence;
    private final NovaSlice[] Slices;
    private int LastTime = 0;
    public NovaManagerNoTap(BlockMemoryAllocator allocator) {
    	
        this.releaseLists = new ArrayList[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
        this.Slices = new NovaSlice[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];
        this.EpochFence = new AtomicLongArray(_Global_Defs.MAX_THREADS * _Global_Defs.CACHE_PADDING * 2);
        this.allocator = allocator;

        for (int i = _Global_Defs.CACHE_PADDING; i < _Global_Defs.MAX_THREADS * _Global_Defs.CACHE_PADDING* 2; i+=_Global_Defs.CACHE_PADDING) {
            this.releaseLists[i]	= new ArrayList<>(_Global_Defs.RELEASE_LIST_LIMIT);
            this.Slices		 [i]	= new NovaSlice(INVALID_SLICE,INVALID_SLICE,INVALID_SLICE);
            this.EpochFence			.set(i, 0);
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
    
    
    public void release(int block, int offset, int len, int idx, int born) {
    	
        List<DeletedSlice> myReleaseList = this.releaseLists[(idx+1)*_Global_Defs.CACHE_PADDING];
        myReleaseList.add(new DeletedSlice(block,offset,len,born,globalNovaNumber.get()));
        
        if (myReleaseList.size() >=  LastTime + _Global_Defs.RELEASE_LIST_LIMIT) {
        	
        	globalNovaNumber.incrementAndGet();
        	EpochFence.set((idx+1)*_Global_Defs.CACHE_PADDING,getNovaEra());
        	Iterator<DeletedSlice> itr=myReleaseList.iterator();
        	while(itr.hasNext()) {
        		DeletedSlice tmp=itr.next();
        		if(!intersects(tmp)) {
        			allocator.free((NovaSlice)tmp);
        			itr.remove();	
        			}
    			}
        	}
        LastTime = myReleaseList.size();
        }
    
    public boolean free(NovaSlice s) {
    	allocator.free(new NovaSlice(s));
    	return true; //assumes always successful!
    }

    public long getAdress(int blockID) {
    	return allocator.getAddress(blockID);
    }


    public NovaSlice getSlice(int size,int ThreadIdx) {
    	NovaSlice s = Slices[(ThreadIdx+1)*_Global_Defs.CACHE_PADDING];
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
    	for(int i= _Global_Defs.CACHE_PADDING; i < 2*_Global_Defs.CACHE_PADDING*_Global_Defs.MAX_THREADS; i +=_Global_Defs.CACHE_PADDING ) {
    		for(NovaSlice x : releaseLists[i]) {
    			allocator.free(x);
    		}
    	}
    }
    
    
    //FenceFree impl
    public class DeletedSlice extends NovaSlice {
    	
        protected long born;
        protected long death;
        
        DeletedSlice(int block, int offset, int length, long born, long died){
        	super(block, offset, length);
        	this.born = born;
        	this.death = died;
        }
    }
    
    public boolean checkEpochFences_inc(int SliceEpoch,int ThreadIdx) {
    	if(EpochFence.get((ThreadIdx+1)*_Global_Defs.CACHE_PADDING) != SliceEpoch) {
    		EpochFence.set((ThreadIdx+1)*_Global_Defs.CACHE_PADDING,getNovaEra());
    		return false;
    	}
    	else return true;
    }
    
    private boolean intersects(DeletedSlice s) {
    	for (int i=0; i<_Global_Defs.MAX_THREADS; i++) {
    		if(s.born <= EpochFence.get((i+1)*_Global_Defs.CACHE_PADDING) 
    				&&  s.death >= EpochFence.get((i+1)*_Global_Defs.CACHE_PADDING))
    			return true;
    	}
    	return false;
    }
    
}
