package com.yahoo.oak;


import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;



public class NovaListBenchmark {
		
	static final int NUM_THREADS=1;
	static final int LIST_SIZE=1000000;
	static final int RUNS= 10;

    public NovaListBenchmark(){    }

    
	public long ReadWriteGeneric(ListInterface list) throws InterruptedException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    AtomicInteger index= new AtomicInteger(0);
	    
		for (int i=0; i<LIST_SIZE; i++)
			list.add((long)i);
		
		for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new GenrticThread(latch,list,index)));
	        threads.get(i).start();
	    }	   
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();

        System.out.println((endTime - startTime));
        return (endTime - startTime);
      
	}
    
    public class GenrticThread implements Runnable{
        ListInterface list;
    	CountDownLatch latch;
    	AtomicInteger index;
        public int idx;
        
        GenrticThread(CountDownLatch latch,ListInterface list,AtomicInteger index) {
            this.latch = latch;
            this.list = list;
            this.index = index;
        }
        
        @Override
        public void run() {
        	initThreads();
        	
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        	for(int i=idx ; i<list.getSize(); i=i+NUM_THREADS ) {
        		//list.set(i, list.get(i)*2);
        		//list.get(i);
        		list.set(i, 2);
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }
    

	
	
    public  void main() {
    	try {
    	long NovaTime=0;
    	long Unmanaged = 0;
		for (int i=0; i<RUNS ; i++) {
            System.out.println("Nova:");
            NovaTime+=ReadWriteGeneric(new NovaList());
            System.out.println("Unmanaged:");
            Unmanaged+=ReadWriteGeneric(new OffHeapList());
		}
        System.out.println("Nova     :"+ NovaTime/RUNS);
        System.out.println("Unmanaged:"+Unmanaged/RUNS);
        	
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

}












/*------------------------------------------NOVA----------------------------------------------------------*/	
//
//	public void ReadWriteNova() throws InterruptedException{
//		CountDownLatch latchNova = new CountDownLatch(NUM_THREADS);
//    	Nova_list= new NovaList();
//		for (int i=0; i<LIST_SIZE; i++)
//			Nova_list.add((long)i);
//		
//	    for (int i = 0; i < NUM_THREADS; i++) {
//	        threads.add(new Thread(new NovaThread(latchNova)));
//	        threads.get(i).start();
//	    }
//	    for (int i=0; i<=NUM_THREADS; i++)
//	    	latchNova.countDown();
//
//        final long startTime = System.nanoTime();
//
//        for (int i = 0; i < NUM_THREADS; i++) {
//            threads.get(i).join();
//        }
//        final long endTime = System.nanoTime();
//
//        System.out.println("Nova	  Heap : " + (endTime - startTime));
//        
//        //---shared with the other List-----//
//        threads.clear();
//        index.set(0);
//		
//	}
//	
//	
//    public class NovaThread implements Runnable{
//        CountDownLatch latch;
//        public int idx;
//        
//        NovaThread(CountDownLatch latch) {
//            this.latch = latch;
//        }
//        
//        @Override
//        public void run() {
//        	initThreads();	
//        	try {
//            	latch.await();
//        	} catch (Exception e) {
//        		e.printStackTrace();
//        		}
//        	
//        	for(int i=idx ; i<Nova_list.getSize(); i=i+NUM_THREADS ) {
//        		//Nova_list.set(i, Nova_list.get(i)*2);
//        			Nova_list.get(i);
//        			//Nova_list.set(i, 2);
//        	}
//        }
//    	public void initThreads() {
//    		idx=index.getAndAdd(1);
//    	}
//    }
///*------------------------------------------NOVADone----------------------------------------------------------*/	
//    
//    
///*------------------------------------------Unmanaged-----------------------------------------------------------*/	
//	public void ReadWriteUnmanaged() throws InterruptedException{
//		CountDownLatch latchUnmanaged = new CountDownLatch(NUM_THREADS);
//
//		Off_list = new OffHeapList();
//		for (int i=0; i<LIST_SIZE; i++)
//			Off_list.add((long)i);
//		
//		for (int i = 0; i < NUM_THREADS; i++) {
//	        threads.add(new Thread(new UnmanagedThread(latchUnmanaged)));
//	        threads.get(i).start();
//	    }	   
//	    for (int i=0; i<=NUM_THREADS; i++)
//	    	latchUnmanaged.countDown();
//
//	    
//        final long startTime = System.nanoTime();
//
//        for (int i = 0; i < NUM_THREADS; i++) {
//            threads.get(i).join();
//        }
//        final long endTime = System.nanoTime();
//
//        System.out.println("Unmanaged Heap : " + (endTime - startTime));
//      
//        //---shared with the other List-----//
//        threads.clear();
//        index.set(0);	
//	}
//	
//    public class UnmanagedThread implements Runnable{
//        CountDownLatch latch;
//
//        public int idx;
//        
//        UnmanagedThread(CountDownLatch latch) {
//            this.latch = latch;
//        }
//        
//        @Override
//        public void run() {
//        	initThreads();
//        	
//            try {
//                latch.await();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            
//        	for(int i=idx ; i<Off_list.getSize(); i=i+NUM_THREADS ) {
//        		//Off_list.set(i, Off_list.get(i)*2);
//        		Off_list.get(i);
//        		//Off_list.set(i, 2);
//        	}
//        }
//    	public void initThreads() {
//    		idx=index.getAndAdd(1);
//    	}
//    }
//    /*------------------------------------------UnmanagedDone-----------------------------------------------------------*/	

