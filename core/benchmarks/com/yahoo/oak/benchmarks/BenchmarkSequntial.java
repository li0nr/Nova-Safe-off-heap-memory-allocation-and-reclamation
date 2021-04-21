package com.yahoo.oak.benchmarks;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.yahoo.oak.ListInterface;
import com.yahoo.oak.List_Nova;
import com.yahoo.oak.List_OffHeap;

import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;





public class BenchmarkSequntial {
		
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=50_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static 	int Limit = 0;

    public BenchmarkSequntial(){    }

    
	public long ReadWriteGeneric(ListInterface list,String s,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    
		
		for (int i = 0; i < NUM_THREADS; i++) {
	        if(s.equals("W"))
	        	threads.add(new Thread(new GenericThreadW(latch,list,i)));
	        if(s.equals("R"))
	        	threads.add(new Thread(new GenericThreadR(latch,list,i)));

	        threads.get(i).start();
	    }	   
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();

        final long startTime = System.nanoTime();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        final long endTime = System.nanoTime();
        if(myWriter!= null) {
        	myWriter.write((endTime - startTime)+"\n");
        }
        return (endTime - startTime);
      
	}
    
  

    
    public  void RunBenchmark(	String list, String mode, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= lenght;
        NUM_THREADS=threads;
        Limit = LIST_SIZE/NUM_THREADS;
        FileWriter myWriter = new FileWriter(list+"_"+mode+"_"+threads+".txt");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova 
        		List_Nova nova=new List_Nova(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i,0);
//		        System.out.println("finsished init\n");

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( nova,mode,myWriter);
	        	}
	        	for (int j=0; j<7 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( nova,mode,myWriter);
	                Mean.add(Time);
	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		List_OffHeap un=new List_OffHeap();
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i,0);
		        System.gc();


	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( un,mode,myWriter);
	        	}
	        	for (int j=0; j<7 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( un,mode,myWriter);
	                Mean.add(Time);
	        	}
        	//	un.close();
	        }
	        myWriter.write(list+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        									+" mode:"+mode+" thread num:"+threads+ "\n");
	        System.out.println(list+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        									+" mode:"+mode+" thread num:"+threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

    //java -cp target/nova-0.0.1-SNAPSHOT.jar -server com.yahoo.oak.BenchmarkSequntial N W 32 1000000

    public static void main(String[] args)throws java.io.IOException {
    	int lenght = 1000;
    	if(args[0]==null) {
    		System.out.print("No args !\n");
    	}
    	String List	=	args[0];
    	String mode	=	args[1];
    	int threads	= 	Integer.parseInt(args[2]);
    	if(args.length == 4) 
    		lenght 	= 	Integer.parseInt(args[3]);
    	BenchmarkSequntial benchmark= new BenchmarkSequntial();
    	benchmark.RunBenchmark(List, mode, lenght, threads);
    	
    }
    
    
    /****************************************************************************/
    public class GenericThreadW extends bench_Thread{
    	GenericThreadW(CountDownLatch latch,ListInterface list,int index) {
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
        		list.set(i, i,idx);
        		}
        	}
        }
    
    public class GenericThreadR extends bench_Thread{
    	GenericThreadR(CountDownLatch latch,ListInterface list,int index) {
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
            	list.get(i,idx);
            	}
            }
        }
  
}