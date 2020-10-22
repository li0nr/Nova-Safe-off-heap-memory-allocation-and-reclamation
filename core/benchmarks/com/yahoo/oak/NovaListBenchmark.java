package com.yahoo.oak;


import java.util.ArrayList;
import java.util.List;
import 	java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.FileWriter;   // Import the FileWriter class
import org.junit.Test;




public class NovaListBenchmark {
		
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=1000000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static int Limit = 0;

    public NovaListBenchmark(){    }

    
	public long ReadWriteGeneric(ListInterface list,String s) throws InterruptedException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    AtomicInteger index= new AtomicInteger(0);
	    
		for (int i=0; i<LIST_SIZE; i++)
			list.add((long)i);
		
		for (int i = 0; i < NUM_THREADS; i++) {
	        if(s.equals("W"))threads.add(new Thread(new GenericThreadW(latch,list,index)));
	        if(s.equals("R"))threads.add(new Thread(new GenericThreadR(latch,list,index)));
	        if(s.equals("RW"))threads.add(new Thread(new GenericThreadRW(latch,list,index)));

	        threads.get(i).start();
	    }	   
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();

    //    System.out.println((endTime - startTime));
        return (endTime - startTime);
      
	}
    
    public class GenericThreadW extends GenericBenchmark{
    	GenericThreadW(CountDownLatch latch,ListInterface list,AtomicInteger index) {
    		super(latch, list, index);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        	for(int i=idx*Limit ; i<idx*Limit  +Limit && i<LIST_SIZE; i=i+1 ) {
        		list.set(i, 2);
        	}
        }

    }
    public class GenericThreadR extends GenericBenchmark{
    	GenericThreadR(CountDownLatch latch,ListInterface list,AtomicInteger index) {
    		super(latch, list, index);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        	for(int i=idx*Limit ; i<idx*Limit  +Limit && i<LIST_SIZE; i=i+1 ) {
        		list.get(i);
        	}
        }

    }
    public class GenericThreadRW extends GenericBenchmark{
    	GenericThreadRW(CountDownLatch latch,ListInterface list,AtomicInteger index) {
    		super(latch, list, index);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        	//for(int i=idx*Section ; i<Limit; i=i+1 ) {
            for(int i=idx*Limit ; i<idx*Limit  +Limit && i<LIST_SIZE; i=i+1 ) {
            	list.set(i, list.get(i)*2);
        	}
        }

    }
  

	
	
    public  void RunBenchmark(int Threads, int items, FileWriter f,String s)throws java.io.IOException {
        ArrayList<Long>	 NovaMean = new ArrayList<>();
        ArrayList<Long>  UnmanagedMean = new ArrayList<>();
        LIST_SIZE= items;
        NUM_THREADS=Threads;
        Limit = LIST_SIZE/NUM_THREADS+1;
        FileWriter myWriter = new FileWriter("results.txt");
        
        try {
    	System.out.println("----------------Threads:  "+Threads+"  Mode:"+s+"----------------");
		long NovaTime=0;
		long Unmanaged = 0;
    	for (int j=0; j<100 ; j++) {
            //System.out.println("Nova:");
    		Thread.sleep(1000);
    		NovaList nova=new NovaList();
            NovaTime=ReadWriteGeneric( nova,s);
            nova.close();
            //System.out.println("Unmanaged:");
    		Thread.sleep(1000);
    		OffHeapList off = new OffHeapList();
            Unmanaged=ReadWriteGeneric(off,s);
            off.close();

            NovaMean.add(NovaTime);
            UnmanagedMean.add(Unmanaged);
    	}
      myWriter.write("Nova  Mean:"+Mean(NovaMean)+" SE:"+StandardError(NovaMean)+" mode:"+s+" thread num:"+Threads+ "\n");
      myWriter.write("Unman Mean:"+Mean(UnmanagedMean)+" SE:"+ StandardError(UnmanagedMean) + " mode:"+s+" thread num:"+Threads+ "\n");
        System.out.println("Nova  Mean:"+Mean(NovaMean)+" SE:"+StandardError(NovaMean)+" mode:"+s+" thread num:"+Threads+ "\n");
        System.out.println("Unman Mean:"+Mean(UnmanagedMean)+" SE:"+ StandardError(UnmanagedMean) + " mode:"+s+" thread num:"+Threads+ "\n");
        NovaMean.clear();
        UnmanagedMean.clear();
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
      myWriter.close();
    }
    @Test
    public void test() {
    	
        int i=0;
        ThreadLocalRandom random = ThreadLocalRandom.current();  
        while(i<100) {
        	System.out.println("round"+i+"value"+random.current().nextInt(1000_000)+"\n");
        	i++;
        }
        
        ArrayList<Long> a= new ArrayList<Long>();
    	for ( i=0; i<10 ; i++) {
    		long n = (i);
    		a.add(n);
    	}
    	double dt=StandardDeviation(a);
    	double se=StandardError(a);
    	double mean= Mean(a);
    	System.out.println("Mean:"+mean);
    	System.out.println("sd :"+dt);
    	System.out.println("se :"+se);
    }
    
    private double Mean(List<Long> means) {
    	int n=0;
//    	for (Long s : means) {
//    		System.out.println("**"+s+"**");
//    	}
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
    
    private double StandardError(List<Long> means) {
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
		mean=sum/(n-1);
		mean=Math.sqrt(mean);
		return mean/Math.sqrt(n);
		
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

