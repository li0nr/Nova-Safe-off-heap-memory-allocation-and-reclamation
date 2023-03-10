/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.nova;

/**
 * This is an interface to be implemented as an alternative to OakNativeMemoryAllocator.
 * Its purpose is to allocate new buffers (DirectByteBuffer) which will be delivered as a Slice object.
 * The allocator can also recycle the memory returned as a Slice object, given this allocation is no longer in use by
 * any thread.
 * Note that two allocations cannot be merged into a single allocation, and an allocation currently is not split.
 */
interface BlockMemoryAllocator {

    // Allocates a portion of a block of the given size, thread safe.    
    boolean allocate(NovaSlice s, int size);

    // Releases a portion of a block (makes it available for reuse) without other GC consideration.
    // IMPORTANT: it is assumed free will get an allocation only initially allocated from this Allocator!    
    void free(NovaSlice s);


    // Is invoked when entire OakMap is closed
    void close();

    // Returns the memory allocation of this OakMap (this Allocator)
    long allocated();

    // Read the buffer from an allocation object that its parameters are set: blockID, offset and length        
    long getAddress(int blockID);
        
    int getBlocks();

    // Check if this Allocator was already closed
    boolean isClosed();
}
