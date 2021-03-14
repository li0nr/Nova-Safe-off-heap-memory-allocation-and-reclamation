package com.yahoo.oak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;





public class BenchmarkSequntial__HE {
		
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=50_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static 	int Limit = 0;

    public BenchmarkSequntial__HE(){    }

    
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
    
  

    
    public  void RunBenchmark( String mode, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= lenght;
        NUM_THREADS=threads;
        Limit = LIST_SIZE/NUM_THREADS;
        FileWriter myWriter = new FileWriter("HE"+"_"+mode+"_"+threads+".txt");
		long Time=0;
		try {
        		ListHE HE=new ListHE(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			HE.add((long)i,0);
//		        System.out.println("finsished init\n");

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( HE,mode,myWriter);
	        	}
	        	for (int j=0; j<7 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( HE,mode,myWriter);
	                Mean.add(Time);
	        	}
	        	HE.close();


	        myWriter.write("HE"+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        									+" mode:"+mode+" thread num:"+threads+ "\n");
	        System.out.println("HE"+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        									+" mode:"+mode+" thread num:"+threads+ "\n");
	        myWriter.close();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

    
    public static void main(String[] args)throws java.io.IOException {
    	int lenght = 1000;
    	if(args[0]==null) {
    		System.out.print("No args !\n");
    	}
    	String mode	=	args[0];
    	int threads	= 	Integer.parseInt(args[1]);
    	if(args.length == 3) 
    		lenght 	= 	Integer.parseInt(args[2]);
    	BenchmarkSequntial__HE benchmark= new BenchmarkSequntial__HE();
    	benchmark.RunBenchmark(mode, lenght, threads);
    	
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