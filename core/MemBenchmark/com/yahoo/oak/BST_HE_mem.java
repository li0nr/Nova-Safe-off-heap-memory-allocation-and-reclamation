package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Random;

import javax.swing.plaf.SliderUI;

import com.yahoo.oak.BST.BST_HE_;
import com.yahoo.oak.Buff.Buff;


public class BST_HE_mem {
	
	static private BST_HE_<Buff,Buff> BSTX ;
    static final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);

	    
		public static class BenchmarkState {
	    	public static  int LIST_SIZE = ParamBench.size;
	    	public static int success_del =0;
	    	public static int failed_del =0;
	    	
	    	public static int success_put =0;
	    	public static int failed_put =0;
	    	
	        static public void setup() {
	    	    
	    	    BSTX = new BST_HE_<Buff,Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER,
	    	    		 Buff.DEFAULT_C,  Buff.DEFAULT_C, allocator);
	    	    
	    	    ParamBench.PrintMem(allocator);
	        	for (int i=0; i <LIST_SIZE ; i++) {
	        		Buff k = new Buff();
	        		k.set(i);
	        		BSTX.put(k,k, 0);
	        	}
	        }
	    }

		

		
		 public static void main(String[] args) throws InterruptedException{
			BenchmarkState.setup();
    	    ParamBench.PrintMem(allocator);
			System.gc();
    	    ParamBench.PrintMem(allocator);

		    ArrayList<Thread> threads = new ArrayList<>();
//		    Random rng = new Random();
//		    Thread.sleep(60000);

//			for (int i = 0; i < 1; i++) 
//				threads.add(new Thread(new writeThread(0,rng.nextLong())));
//				
//			for (int i = 0; i < 1; i++) 
//				threads.add(new Thread(new delThread(1,rng.nextLong())));
//				
//
//			for (Thread x : threads) {
//				x.start();
//			}
			
		}
		 
		 
		 public static class bench_Thread implements Runnable{
			 Random random;
			 public com.yahoo.oak.Buff.Buff iB;
			 public int idx;
			    
			 bench_Thread(int index, long seed) {
				 iB = new com.yahoo.oak.Buff.Buff();
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
	        		iB.set(random.nextInt(BenchmarkState.LIST_SIZE));
	        		if(BSTX.remove(iB, idx))
	        			BenchmarkState.success_del++;
	        		else 
	        			BenchmarkState.failed_del++;
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
	        		iB.set(random.nextInt(BenchmarkState.LIST_SIZE));
	        		if(BSTX.put(iB,iB, idx) == true)
	        			BenchmarkState.success_put++;
	        		else 
	        			BenchmarkState.failed_put++;

	    		}
	    	}
	    }
}
