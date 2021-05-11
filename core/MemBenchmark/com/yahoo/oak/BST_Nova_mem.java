package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import com.yahoo.oak.*;

public class BST_Nova_mem {
    static private BST_Nova<Buff,Buff> BSTX ;
    static final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);

    
	public static class BenchmarkState {
    	public static  int LIST_SIZE = ParamBench.size;

        static public void setup() {
    	    final NovaManager mng = new NovaManager(allocator);
    	    
    	    BSTX = new BST_Nova<Buff,Buff>(Buff.DEFAULT_COMPARATOR, Buff.DEFAULT_COMPARATOR
					, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER,Buff.DEFAULT_C,Buff.DEFAULT_C,mng);
        	for (int i=0; i <LIST_SIZE ; i++) {
        		Buff k = new Buff();
        		k.set(i);
        		//BSTX.put(k,k, 0);
        	}
        }
    }

	

	
	 public static void main(String[] args){
		BenchmarkState.setup();
		final int M = 1024*1024;
        long heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        long heapFreeSize = Runtime.getRuntime().freeMemory();

        double usedHeapMemoryMB = (double) (heapSize - heapFreeSize) / M;
        double usedOffHeapMemoryMB = (double) ( allocator.allocated()) / M;
        
        double heapOverhead = usedHeapMemoryMB / (usedHeapMemoryMB + usedOffHeapMemoryMB);
        System.out.println("Observed OnHeap :"+ usedHeapMemoryMB);
        System.out.println("Observed OffHeap :"+ usedOffHeapMemoryMB);

	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();

		for (int i = 0; i < 1; i++) 
			threads.add(new Thread(new writeThread(0,rng.nextLong())));
			
		for (int i = 0; i < 1; i++) 
			threads.add(new Thread(new delThread(1,rng.nextLong())));
			

		for (Thread x : threads) {
			x.start();
		}
	}
	 
	 
	 public static class bench_Thread implements Runnable{
		 Random random;
		 public com.yahoo.oak.Buff iB;
		 public int idx;
		    
		 bench_Thread(int index, long seed) {
			 iB = new com.yahoo.oak.Buff();
			 idx=index;
			 random = new Random(seed);
			 }
		 @Override
		 public void run() {}
	 }
	 
	 
	 public static class delThread extends bench_Thread {

		 delThread(int index, long seed) {
			super(index, seed);
			}
	
    	@Override
		public void run() {
    		while (true) {
        		iB.set(random.nextInt(1000));
        		BSTX.remove(iB, idx);
    		}
    	}
    }
	 
	 
	 public static class writeThread extends bench_Thread {

		 writeThread(int index, long seed) {
			super(index, seed);
			}
	
    	@Override
		public void run() {
    		while(true) {
        		iB.set(random.nextInt(1000));
        		BSTX.put(iB,iB, idx);	
    		}
    	}
    }
	 
}

		
		

