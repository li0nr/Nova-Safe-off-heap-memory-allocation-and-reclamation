package com.yahoo.oak;

//import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

//import org.junit.Test;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors



public class NovaListTest {
	
	FileWriter myWriter ;
	
	static final int NUM_THREADS=1;
	static final int LIST_SIZE=1000000;

    private  ArrayList<Thread> threads;
    
    private static CountDownLatch latchOFF = new CountDownLatch(1);
    private static CountDownLatch latchON = new CountDownLatch(1);

    private static AtomicInteger index= new AtomicInteger(0);
    
    NovaList Nova_list;
    OffHeapList Off_list;
    ArrayList<Integer> On_list;
    
    public NovaListTest(){
    	Nova_list= new NovaList();
    	//On_list = new  ArrayList<Integer>();
    	Off_list = new OffHeapList();
        threads = new ArrayList<>(NUM_THREADS);
    }

	
    public class RunThreads implements Runnable{
        CountDownLatch latch;
        CyclicBarrier barrier;

        public int idx;
        
        public int getidx() {
        	return idx;
        }
        RunThreads(CountDownLatch latch, CyclicBarrier barrier) {
            this.latch = latch;
            this.barrier = barrier;

        }
        @Override
        public void run() {
        	initThreads();
        	
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            
        	for(int i=idx ; i<Nova_list.getSize(); i=i+NUM_THREADS ) {
        		try {
        			Nova_list.set(i, Nova_list.get(i)*2);
        		}catch(Exception e) {
        			
        		}
            	
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }


    public class RunThreadsOff implements Runnable{
        CountDownLatch latch;
        CyclicBarrier barrier;

        public int idx;
        
        public int getidx() {
        	return idx;
        }
        RunThreadsOff(CountDownLatch latch, CyclicBarrier barrier) {
            this.latch = latch;
            this.barrier = barrier;

        }
        @Override
        public void run() {
        	initThreads();
        	
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            
        	for(int i=idx ; i<Off_list.getSize(); i=i+NUM_THREADS ) {
        		
        		Off_list.set(i, Off_list.get(i)*2);
            	
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }
	
    
    
	void initlist() {
		for (int i=0; i<LIST_SIZE; i++)
			Nova_list.add(i);
		
	}

	void initOnlist() {
		for (int i=0; i<LIST_SIZE; i++)
			On_list.add(i);
		
	}
	
	void initOFFlist() {
		for (int i=0; i<LIST_SIZE; i++)
			Off_list.add(i);
		
	}
	
	
	
	public void ReadWrite() throws InterruptedException{
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

		initlist();
		
		
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new RunThreads(latchOFF,barrier)));
	        threads.get(i).start();
	    }
	    latchOFF.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();

        System.out.println("Total execution time Off Heap : " + (endTime - startTime));
        try {
        	String S="Total execution time Off Heap : " + (endTime - startTime);
            myWriter.write(S);
            System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
        for (int i = 0; i <LIST_SIZE; i++) {
 //       	assertEquals(Off_list.get(i), i*2, "ojk");
        }
		
	}
	
//	@Test
	
	public void RunThreadsOff() throws InterruptedException{
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

		initOnlist();
		
		
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new RunThreadsOff(latchON,barrier)));
	        threads.get(i).start();
	    }
	    latchON.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();

        
        try {
        	String S="Total execution time On Heap : " + (endTime - startTime);
        	System.out.println(S);
            myWriter.write(S);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
        for (int i = 0; i <LIST_SIZE; i++) {
  //      	assertEquals(On_list.get(i), i*2, "ok");
        }
		
	}
	
	public void ReadWriteOff() throws InterruptedException{
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);

		initOFFlist();
		
		
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new RunThreadsOff(latchON,barrier)));
	        threads.get(i).start();
	    }
	    latchON.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();

        
        try {
        	String S="Total execution time On Heap : " + (endTime - startTime);
        	System.out.println(S);
            myWriter.write(S);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
        for (int i = 0; i <LIST_SIZE; i++) {
  //      	assertEquals(On_list.get(i), i*2, "ok");
        }
		
	}
	
	
    public  void main() {
        try {
        	myWriter = new FileWriter("filename.txt");
         } catch (IOException e) {
           System.out.println("An error occurred.");
           e.printStackTrace();
         }
    	try {
        	ReadWrite();
        	threads.clear();
        	index.set(0);
        	ReadWriteOff();

    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

}
