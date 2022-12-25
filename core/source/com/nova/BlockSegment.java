/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.nova;


import java.util.concurrent.atomic.AtomicInteger;

import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

class BlockSegment {

	private int id;
    private final MemorySegment seg;
    private final int capacity;
    private final AtomicInteger allocated = new AtomicInteger(0);


    BlockSegment(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE; // This is exactly 2GB
        this.capacity = (int) capacity;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        ResourceScope scope = ResourceScope.newSharedScope();
        this.seg  = MemorySegment.allocateNative(this.capacity, scope);
    }

    void setID(int id) {
    	this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    MemorySegment allocate(int size) {
        int now = allocated.getAndAdd(size );
        if (now + size  > this.capacity) {
            allocated.getAndAdd(-size );
            throw new OakOutOfMemoryException();
        }
        ResourceScope scope = ResourceScope.newSharedScope();
        MemoryAddress MemAddress = this.seg.address().addOffset(now);
        return MemAddress.asSegment(size,scope);
    }



    // use when this Block is no longer in any use, not thread safe
    // It sets the limit to the capacity and the position to zero, but didn't zeroes the memory
    void reset() {
        allocated.set(0);
    }

    // return how many bytes are actually allocated for this block only, thread safe
    long allocated() {
        return allocated.get();
    }

    // releasing the memory back to the OS, freeing the block, an opposite of allocation, not thread safe
    void clean() {
    	this.seg.scope().close();
    }


    // how many bytes a block may include, regardless allocated/free
    public int getCapacity() {
        return capacity;
    }

    public MemorySegment getSegment() {
    	return this.seg;
    }
    
    public long getAddress() {
        throw new RuntimeException();
    }

}
