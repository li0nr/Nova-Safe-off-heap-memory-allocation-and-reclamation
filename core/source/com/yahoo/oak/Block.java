/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import java.util.concurrent.atomic.AtomicInteger;

class Block {

	private int id;
    private final long address;
    private final int capacity;
    private final AtomicInteger allocated = new AtomicInteger(0);
    
    
    Block(long capacity) {
        assert capacity > 0;
        assert capacity <= Integer.MAX_VALUE; // This is exactly 2GB
        this.capacity = (int) capacity;
        this.id = NativeMemoryAllocator.INVALID_BLOCK_ID;
        // Pay attention in allocateDirect the data is *zero'd out*
        // which has an overhead in clearing and you end up touching every page
        this.address = UnsafeUtils.unsafe.allocateMemory(this.capacity);
    }
    
    void setID(int id) {
    	this.id = id;
    }

    // Block manages its linear allocation. Thread safe.
    boolean allocate(NovaSlice s, int size) {
        int now = allocated.getAndAdd(size );
        if (now + size  > this.capacity) {
            allocated.getAndAdd(-size );
            throw new OakOutOfMemoryException();
        }
        s.update(id , now, size, address);
        return true;
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
    	UnsafeUtils.unsafe.freeMemory(address);
    }


    // how many bytes a block may include, regardless allocated/free
    public int getCapacity() {
        return capacity;
    }
    
    public long getAddress() {
        return address;
    }

}
