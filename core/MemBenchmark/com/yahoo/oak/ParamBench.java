package com.yahoo.oak;

public class ParamBench {

	static final int size = 100000;
	
	
	public static void PrintMem(NativeMemoryAllocator allocator) {
		final int M = 1024*1024;
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        double usedHeapMemoryMB = (double) (heapSize - heapFreeSize) / M;
        double usedOffHeapMemoryMB = (double) ( allocator.allocated()) / M;
        
        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("\n Observed OnHeap :"+ usedHeapMemoryMB);
        System.out.println("\n Observed OffHeap :"+ usedOffHeapMemoryMB);
	}
}
