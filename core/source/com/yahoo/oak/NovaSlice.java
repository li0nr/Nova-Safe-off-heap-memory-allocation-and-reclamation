/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.nio.ByteBuffer;


// Represents a portion of a bigger block which is part of the underlying managed memory.
// It is allocated via block memory allocator, and can be de-allocated later
class NovaSlice implements  Comparable<NovaSlice> {

    /**
     * An allocated slice might have reserved space for meta-data, i.e., a header.
     * In the current implementation, the header size is defined externally at the construction.
     * In future implementations, the header size should be part of the allocation and defined
     * by the allocator/memory-manager using the update() method.
     */
	protected final static int headerSize=8;

    protected int blockID;
    protected int offset;
    protected int length;
    protected long address;

    protected ByteBuffer buffer;

    
    NovaSlice(int block, int offset, int len) {
        blockID=block;
        this.offset=offset;
        this.length= len;
    }

    // Used to duplicate the allocation state. Does not duplicate the buffer itself.
    NovaSlice(NovaSlice otherSlice) {
        copyFrom(otherSlice);
    }

    // Used by OffHeapList in "synchrobench" module, and for testings.
    void duplicateBuffer() {
        buffer = buffer.duplicate();
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length) {
//        assert headerSize <= length;
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;
        this.buffer = null;
    }
    
    void setAddress(long address) {
    	this.address = address;
    }

    // Copy the block allocation information from another block allocation.
    void copyFrom(NovaSlice other) {
        if (other == this) {
            // No need to do anything if the input is this object
            return;
        }
        this.blockID = other.blockID;
        this.offset = other.offset;
        this.buffer = other.buffer;
        this.length = other.length;
    }

    // Set the internal buffer.
    // This method should be used only by the block memory allocator.
    void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    void setLen(int len) {
    	length=len;
    }
    void setHeader(int version, int size) {
    	long header_slice= (size+headerSize) <<24 & 0xFFFFF000;
    	int newVer= (version<<1 | 0) & 0xFFF;
    	long header=header_slice | newVer ;
    	UnsafeUtils.unsafe.putLong(address+offset, header);
    }

//    /* ------------------------------------------------------------------------------------
//     * Allocation info getters
//     * ------------------------------------------------------------------------------------*/

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }
    
    int getLength() {
        return length;
    }

    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/

//    boolean isValidVersion() {
//    //    return version != EntrySet.INVALID_VERSION;
//    }
//
    long getVersion() {
    	return UnsafeUtils.unsafe.getLong(address+offset)& 0xFFFFFF;
    }
	  long getRef() {
		  int ref=blockID;
		  return ref<<20 | offset;
	 }

    long getMetadataAddress() {
        return ((DirectBuffer) buffer).address() + offset;
    }

    /*-------------- OakUnsafeDirectBuffer --------------*/

//    @Override
//    public ByteBuffer getByteBuffer() {
//        return buffer;
//    }
//
//    @Override
//    public int getOffset() {
//        return offset+headerSize;
//    }
//    
//    public int getHeaderOffset() {
//        return offset;
//    }
//
//    @Override
//    public int getLength() {
//    	if(buffer == null) return this.length;
//    	int header= (int) buffer.getLong(offset)>>24;
//    	return header;
//    }
//
//    @Override
//    public long getAddress() {
//        return ((DirectBuffer) buffer).address();
//    }

    /*-------------- Comparable<Slice> --------------*/

    /**
     * The slices are ordered by their length, then by their block id, then by their offset.
     * Slices with the same length, block id and offset are considered identical.
     */
    @Override
    public int compareTo(NovaSlice o) {
        int cmp = Integer.compare(this.length, o.length);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.blockID, o.blockID);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.offset, o.offset);
    }
    

}
