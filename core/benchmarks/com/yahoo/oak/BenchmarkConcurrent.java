package com.yahoo.oak;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;


public class BenchmarkConcurrent {

	
	static  int NUM_THREADS=1;
	static 	int LIST_SIZE =1_000_000;
	static  int Limit = 0;
	static  int rangeforReadWrite=1000; //range for the concurrent test all the 15 reader read from the same 1000section, so does the writer.
	
	static volatile boolean stop=false;

	
	
    public  void ConcurrentReadWriteBenchmark(String list, int threads, String Mode)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        Limit 			= 2_000_000;
        NUM_THREADS	= threads;

        FileWriter myWriter = new FileWriter("Random concurrent Read Write"+".txt");
        System.out.println("concurrent Read Write benchmark\n");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i,0);

	    		for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			ReaderMeasurement(nova,myWriter);
	        		if(Mode.equals("W"))
	        			WriterMeasurement(nova,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			Time =ReaderMeasurement(nova,myWriter);
	        		if(Mode.equals("W"))
	        			Time =WriterMeasurement(nova,myWriter);
	        		Mean.add(Time);
	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		OffHeapList un=new OffHeapList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i,0);

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			ReaderMeasurement(un,myWriter);
	        		if(Mode.equals("W"))
	        			WriterMeasurement(un,myWriter);	  
	        		}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			Time =ReaderMeasurement(un,myWriter);
	        		if(Mode.equals("W"))
	        			Time =WriterMeasurement(un,myWriter);
	                Mean.add(Time);
	        	}
        		un.close();
	        }
	        myWriter.write(list+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        						+" thread num:"+threads+ "\n");
	        System.out.println(list+"Mean:"+bench_Math.Mean(Mean)+" SE:"+bench_Math.StandardError(Mean)
	        						+" thread num:"+threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

	
	public long Measurement(ListInterface list,  FileWriter myWriter , String Mode) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();
	    
	    for (int i = 0; i < NUM_THREADS-2; i++) {
	    	threads.add(new Thread(new ReaderThread(latch,list,i,rng.nextLong(),false)));
	    	threads.get(i).start();
	    	}	 			
    	if(Mode.equals("R")) {
        	threads.add(new Thread(new WriterThread(latch,list,NUM_THREADS-2,rng.nextLong(),false)));
    		threads.add(new Thread(new ReaderThread(latch,list,NUM_THREADS-1,rng.nextLong(),true)));
        	}
    	if(Mode.equals("W")) {
    		threads.add(new Thread(new ReaderThread(latch,list,NUM_THREADS-2,rng.nextLong(),false)));
        	threads.add(new Thread(new WriterThread(latch,list,NUM_THREADS-1,rng.nextLong(),true)));
    		}
	    threads.get(NUM_THREADS-2).start();
    	threads.get(NUM_THREADS-1).start();
     
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();
	    final long startTime = System.nanoTime();
	    
	    threads.get(NUM_THREADS-1).join();
	    stop=true;
	    for (int i = 0; i < NUM_THREADS-1; i++) 
	    	threads.get(i).join();
	    stop=false;
	    final long endTime = System.nanoTime();
	    if(myWriter!= null) {
	    	myWriter.write((endTime - startTime)+"\n");
	    	}
	    return (endTime - startTime);
	}
	
	public long ReaderMeasurement(ListInterface list, FileWriter file) {
		try {
			return Measurement(list, file, "R");
		}catch(Exception e) {}
		return 0;
	}
	
	public long WriterMeasurement(ListInterface list, FileWriter file) {
		try {
			return Measurement(list, file, "W");
		}catch(Exception e) {}
		return 0;
	}	
	
	
	
	
	public static void main(String[] args) throws java.io.IOException {
    	if(args[0]==null) {
    		System.out.print("No args !\n");
    	}
    	String List	=	args[0];
    	String mode	=	args[1];
    	int threads	= 	Integer.parseInt(args[2]);
    	BenchmarkConcurrent test= new BenchmarkConcurrent();
    	test.ConcurrentReadWriteBenchmark(List,threads,mode);
    	
    }
    
    
    /*******************Runnable Threads*********************************************/
    
	
    public class ReaderThread extends bench_Thread{
    	boolean Measured;
    	ReaderThread(CountDownLatch latch,ListInterface list,int index,long seed, boolean Measured) {
    		super(latch, list, index,seed);
    		this.Measured = Measured;
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            int j=0,i=0;
            if (Measured) {
				while(i<Limit) {
					j=random.nextInt(rangeforReadWrite);
					j+=LIST_SIZE/2;
	            	list.get(j,idx);
	            	i++;
	            	}
            }else {
                while(!stop) {
                	j=random.nextInt(rangeforReadWrite);
                	j+=LIST_SIZE/2;
                	list.get(j,idx);
                	i++;
            	}
            }

        }
    }
    
	public class WriterThread extends bench_Thread{
		boolean Measured;
		WriterThread(CountDownLatch latch,ListInterface list,int index,long seed, boolean Measured) {
			super(latch, list, index,seed);
			this.Measured= Measured;
			}  
		@Override
		public void run() {
			try {
				latch.await();
				} catch (Exception e) {
					e.printStackTrace();
					}
			int j=0,i=0;
			if (Measured) {
				while(i<Limit) {
					j=random.nextInt(rangeforReadWrite);
					j+=LIST_SIZE/2;
	            	list.set(j,j,idx);
	            	i++;
	            	}
			}else {
				while(!stop) {
					j=random.nextInt(rangeforReadWrite);
					j+=LIST_SIZE/2;
	            	list.set(j,j,idx);
	            	}
				}
			}

		}
}
