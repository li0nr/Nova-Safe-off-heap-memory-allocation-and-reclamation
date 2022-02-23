package com.yahoo.oak;

import jdk.incubator.foreign.MemorySegment;
import org.junit.Test;

public class Segment_Test {


    @Test
    public void SegmentTest() {
        BlockSegment block = new BlockSegment(1024*8);
        MemorySegment seg = null;
        block.allocate(seg,8);
    }
}
