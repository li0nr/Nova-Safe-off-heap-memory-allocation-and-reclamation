package com.yahoo.oak;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class GenericBenchmark implements Runnable{
    ListInterface list;
	CountDownLatch latch;
	AtomicInteger index;
    ThreadLocalRandom random = ThreadLocalRandom.current();  

    public int idx;
    
    GenericBenchmark(CountDownLatch latch,ListInterface list,AtomicInteger index) {
        this.latch = latch;
        this.list = list;
        this.index = index;
		idx=index.getAndAdd(1);
    }
    
    @Override
    public void run() {}
    
}