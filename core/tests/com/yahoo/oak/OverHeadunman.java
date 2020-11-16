package com.yahoo.oak;


import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class OverHeadunman {
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 10_000_000;
    private static OffHeapList List;

    @Before
    public void init() {
    	List = new OffHeapList();
    	for (int i =0; i<NUM_OF_ENTRIES; i++)
    		List.add((long)i);
    }

    @Test
    public void main() {
    	for (int i =0; i<NUM_OF_ENTRIES; i++)
    		List.set(i,(long)i);
    	
        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        double usedHeapMemoryMB = (double) (heapSize - heapFreeSize) / M;
        double usedOffHeapMemoryMB = (double) (8*NUM_OF_ENTRIES) / M;

        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("Observed Off Heap mem: " + usedOffHeapMemoryMB);
        System.out.println("Observed On Heap mem: " + usedHeapMemoryMB);
        System.out.println("Observed On Heap Overhead: " + heapOverhead);
    }
}
