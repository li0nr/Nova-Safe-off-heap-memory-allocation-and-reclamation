package com.yahoo.oak;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class benchThread implements Runnable{
    ListInterface list;
	CountDownLatch latch;
    Random random;  

    public int idx;
    
    benchThread(CountDownLatch latch,ListInterface list,int index) {
        this.latch = latch;
        this.list = list;
		idx=index;
    }
    
    benchThread(CountDownLatch latch,ListInterface list,int index, long seed) {
        this.latch = latch;
        this.list = list;
		idx=index;
		random = new Random(seed);
    }
    
    @Override
    public void run() {}
    
}