package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Random;

import javax.swing.plaf.SliderUI;


public class BST_HE_mem {
	
	static private BST_HE<Buff,Buff> BSTX ;

	    
		public static class BenchmarkState {
	    	public static  int LIST_SIZE = 20_000;

	        static public void setup() {
	    	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    	    
	    	    BSTX = new BST_HE<Buff,Buff>(Buff.DEFAULT_COMPARATOR, Buff.DEFAULT_COMPARATOR
						, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER,allocator);
	        	for (int i=0; i <LIST_SIZE ; i++) {
	        		Buff k = new Buff();
	        		k.set(i);
	        		BSTX.put(k,k, 0);
	        	}
	        }
	    }

		

		
		 public static void main(String[] args){
			BenchmarkState.setup();
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
