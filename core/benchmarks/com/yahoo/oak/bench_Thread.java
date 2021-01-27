package com.yahoo.oak;

import java.util.Random;
import java.util.concurrent.CountDownLatch;


public class bench_Thread implements Runnable{
    ListInterface list;
	CountDownLatch latch;
    Random random;  

    public int idx;
    
    bench_Thread(CountDownLatch latch,ListInterface list,int index) {
        this.latch = latch;
        this.list = list;
		idx=index;
    }
    
    bench_Thread(CountDownLatch latch,ListInterface list,int index, long seed) {
        this.latch = latch;
        this.list = list;
		idx=index;
		random = new Random(seed);
    }
    
    @Override
    public void run() {}
    
}