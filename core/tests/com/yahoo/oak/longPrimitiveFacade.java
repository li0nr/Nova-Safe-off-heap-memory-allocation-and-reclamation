package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.yahoo.oak.FacadeTest.ReaderThread;

public class longPrimitiveFacade {

	
    private  NovaManager  novaManager;

	private  PrimitiveFacade facade;
    private  ArrayList<Thread> threads;
    private static CountDownLatch latch = new CountDownLatch(1);

    private  final int NUM_THREADS = 3;
    


    private  void initNova() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
         novaManager = new NovaManager(allocator);
         PrimitiveFacade.novaManager = novaManager;
        threads = new ArrayList<>(NUM_THREADS);
    }
    
    
	@Test 
	public void concurrentREAD() throws InterruptedException {
		initNova();   
		long x = 1;
		PrimitiveFacade.AllocateSlice(x, 8, 0);
	}
}
