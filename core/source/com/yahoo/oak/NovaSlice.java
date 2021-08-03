/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


// Represents a portion of a bigger block which is part of the underlying managed memory.
// It is allocated via block memory allocator, and can be de-allocated later
public class NovaSlice implements  Comparable<NovaSlice> {

    /**
     * An allocated slice might have reserved space for meta-data, i.e., a header.
     * In the current implementation, the header size is defined externally at the construction.
     * In future implementations, the header size should be part of the allocation and defined
     * by the allocator/memory-manager using the update() method.
     */
    protected int blockID;
    public int offset;
    protected int length;
    public long address;


    
    public NovaSlice(int block, int offset, int len) {
        blockID=block;
        this.offset=offset;
        this.length= len;
    }

    // Used to duplicate the allocation state. Does not duplicate the buffer itself.
    NovaSlice(NovaSlice otherSlice) {
        copyFrom(otherSlice);
    }

    /* ------------------------------------------------------------------------------------
     * Allocation info and metadata setters
     * ------------------------------------------------------------------------------------*/

    /*
     * Updates the allocation object.
     * The buffer should be set later by the block allocator.
     */
    void update(int blockID, int offset, int length, long address) {
        this.blockID = blockID;
        this.offset = offset;
        this.length = length;
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
        this.length = other.length;
    }

    void setAddress(long address) {
    	this.address = address;
	}


    void setHeader(int version, int size) {
    	long header_slice	= (long)(size+NovaManager.HEADER_SIZE) <<24 & 0xFFFFF000;
    	int  newVer			= (version<<1 | 0) & 0xFFF;
    	long header			= header_slice | newVer ;
    	
    	UnsafeUtils.unsafe.putLong(address+offset, header);
	}
    
    void setHeader_Magic(int version, int size) {
    	long header_slice	= (long)(size+NovaManager.HEADER_SIZE+NovaManager.MAGIC_SIZE) <<24 & 0xFFFFF000;
    	int  newVer			= (version<<1 | 0) & 0xFFF;
    	long header			= header_slice | newVer ;
    	
    	UnsafeUtils.unsafe.putLong(address+offset, NovaManager.MAGIC_NUM); //Magic number changes
    	UnsafeUtils.unsafe.putLong(address+offset+NovaManager.MAGIC_SIZE, header); //Magic number changes		
    }
    
    


    /* ------------------------------------------------------------------------------------
     * Allocation info getters
     * ------------------------------------------------------------------------------------*/

    int getAllocatedBlockID() {
        return blockID;
    }

    int getAllocatedOffset() {
        return offset;
    }
    
    int getLength() {
        return length;
    }
	  
    long getAddress() {
    	return address;
	}



    /* ------------------------------------------------------------------------------------
     * Metadata getters
     * ------------------------------------------------------------------------------------*/

    long getVersion() {
    	return UnsafeUtils.unsafe.getLong(address+offset)& 0xFFFFFF;
	}

    long getVersionMagic() {
    	return UnsafeUtils.unsafe.getLong(address+offset+NovaManager.MAGIC_SIZE)& 0xFFFFFF;
	}
    
    long getRef() {
    	int ref=blockID;
    	return ref<<20 | offset;
	}




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
