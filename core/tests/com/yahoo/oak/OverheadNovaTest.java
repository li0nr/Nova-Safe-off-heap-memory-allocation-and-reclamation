package com.yahoo.oak;


import org.junit.Before;
import org.junit.Test;

public class OverheadNovaTest {
    private static final int K = 1024;
    private static final int M = K * K;
    private static final int NUM_OF_ENTRIES = 10_000_000;
    private static final int KEY_SIZE = 100;
    private static final int VALUE_SIZE = 1000;
    private static final double MAX_ON_HEAP_OVERHEAD_PERCENTAGE = 0.05;
    private static NovaList nList;

    @Before
    public void init() {
    	nList = new NovaList();
    	for (int i =0; i<NUM_OF_ENTRIES; i++)
    		nList.add((long)i,0);
    }

    @Test
    public void main() {
    	for (int i =0; i<NUM_OF_ENTRIES; i++)
    		nList.set(i,(long)i,0);
    
        System.gc();
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        double usedHeapMemoryMB = (double) (heapSize - heapFreeSize) / M;
        double usedOffHeapMemoryMB = (double) (nList.novaManager.allocated()) / M;

        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("Observed Off Heap mem: " + usedOffHeapMemoryMB);
        System.out.println("Observed On Heap mem: " + usedHeapMemoryMB);
        System.out.println("Observed On Heap Overhead: " + heapOverhead);
    }
}
