package com.yahoo.oak;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class benchThread implements Runnable{
    ListInterface list;
	CountDownLatch latch;
	AtomicInteger index;
    Random random;  

    public int idx;
    
    benchThread(CountDownLatch latch,ListInterface list,AtomicInteger index) {
        this.latch = latch;
        this.list = list;
        this.index = index;
		idx=index.getAndAdd(1);
    }
    
    benchThread(CountDownLatch latch,ListInterface list,AtomicInteger index, long seed) {
        this.latch = latch;
        this.list = list;
        this.index = index;
		idx=index.getAndAdd(1);
		random = new Random(seed);
    }
    
    @Override
    public void run() {}
    
}