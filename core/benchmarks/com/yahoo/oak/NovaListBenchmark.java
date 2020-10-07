package com.yahoo.oak;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import java.io.FileWriter;   // Import the FileWriter class



public class NovaListBenchmark {
		
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=1000000;
	static  int RUNS= 10;

    public NovaListBenchmark(){    }

    
	public long ReadWriteGeneric(ListInterface list,String s) throws InterruptedException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    AtomicInteger index= new AtomicInteger(0);
	    
		for (int i=0; i<LIST_SIZE; i++)
			list.add((long)i);
		
		for (int i = 0; i < NUM_THREADS; i++) {
	        if(s.equals("W"))threads.add(new Thread(new GenrticThreadW(latch,list,index)));
	        if(s.equals("R"))threads.add(new Thread(new GenrticThreadR(latch,list,index)));
	        if(s.equals("RW"))threads.add(new Thread(new GenrticThreadRW(latch,list,index)));

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
    
    public class GenrticThreadW implements Runnable{
        ListInterface list;
    	CountDownLatch latch;
    	AtomicInteger index;
        public int idx;
        
        GenrticThreadW(CountDownLatch latch,ListInterface list,AtomicInteger index) {
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

        		list.set(i, 2);
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }
    public class GenrticThreadR implements Runnable{
        ListInterface list;
    	CountDownLatch latch;
    	AtomicInteger index;
        public int idx;
        
        GenrticThreadR(CountDownLatch latch,ListInterface list,AtomicInteger index) {
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
        		list.get(i);
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }
    public class GenrticThreadRW implements Runnable{
        ListInterface list;
    	CountDownLatch latch;
    	AtomicInteger index;
        public int idx;
        
        GenrticThreadRW(CountDownLatch latch,ListInterface list,AtomicInteger index) {
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
        		list.set(i, list.get(i)*2);
        	}
        }
    	public void initThreads() {
    		idx=index.getAndAdd(1);
    	}
    }
    

	
	
    public  void RunBenchmark(int Threads, int items, FileWriter f,String s) {
        ArrayList<Long>	 NovaMean = new ArrayList<>();
        ArrayList<Long>  UnmanagedMean = new ArrayList<>();
        LIST_SIZE= items;
        NUM_THREADS=Threads;
    	try {
    	long NovaTime=0;
    	long Unmanaged = 0;
    	for (int j=0; j<1 ; j++) {
    		for (int i=0; i<1 ; i++) {
                System.out.println("Nova:");
                NovaTime+=ReadWriteGeneric(new NovaList(),s);
                System.out.println("Unmanaged:");
                Unmanaged+=ReadWriteGeneric(new OffHeapList(),s);
    		}
    		Thread.sleep(10000);
            NovaMean.add(NovaTime/10);
            UnmanagedMean.add(Unmanaged/10);
    	}
        f.write("Nova  Mean:"+Mean(NovaMean)+" SE:"+StandardDeviation(NovaMean)+" mode:"+s+" thread num:"+Threads+ "\n");
        f.write("Unman Mean:"+Mean(UnmanagedMean)+" SE:"+ StandardDeviation(UnmanagedMean) + " mode:"+s+" thread num:"+Threads+ "\n");

    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    @Test
    public void test() {
    	ArrayList<Long> a= new ArrayList<Long>();
    	for (int i=0; i<5 ; i++) {
    		long n = (i+1);
    		a.add(n);
    	}
    	double dt=StandardDeviation(a);
    	System.out.println("sd :"+dt);
    }
    
    private double Mean(List<Long> means) {
    	int n=0;
    	double sum=0, mean=0;
    	for (double s : means) {
    		sum=sum+s;
			n++;
			}
		return sum/n;
    }
    
    private double StandardDeviation(List<Long> means) {
    	int n=0;
    	double sum=0, mean=0;
    	for (double s : means) {
    		sum=sum+s;
			n++;
			}
		mean=sum/n;
		sum=0;  
    	for (double s : means) {
			sum+=Math.pow((s-mean),2);
    	}
		mean=sum/(n);
		return Math.sqrt(mean);
		
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

