package com.yahoo.oak;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import org.junit.Test;

public class Segment_Test {


    @Test
    public void SegmentTest() {
        BlockSegment block = new BlockSegment(1024*8);
        MemorySegment seg = null;
        block.allocate(8);
    }
    
    
    @Test
    public void SegmentDeAllocTest() {
        MemorySegmentAllocator block = new MemorySegmentAllocator(1024*8);
        MemorySegment s = block.allocate(8);
        MemoryAddress MemAddress = s.address();
        ResourceScope scope = ResourceScope.newSharedScope();
    	MemorySegment s2 = MemAddress.asSegment(8,scope);
    	MemoryAccess.getIntAtOffset(s,0);    
    	s.scope().close();
    	MemoryAccess.setIntAtOffset(s2,0,8);    
    	try {
    		MemoryAccess.getIntAtOffset(s,0);       
    	} catch (IllegalStateException e) {
    		
    	}

    }
}
