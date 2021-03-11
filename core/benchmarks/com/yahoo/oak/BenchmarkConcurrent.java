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

	
	
    public  void ConcurrentReadWriteBenchmark(String list, int threads, String Mode, int random)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        Limit 			= 10_000_000;
        NUM_THREADS	= threads;

        FileWriter myWriter = new FileWriter("Random_WR"+list+threads+Mode+".txt");
        System.out.println("concurrent Read Write benchmark\n");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova
        		ListNova nova=new ListNova(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i,0);

	    		for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			ReaderMeasurement(nova,myWriter,random);
	        		if(Mode.equals("W"))
	        			WriterMeasurement(nova,myWriter,random);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			Time =ReaderMeasurement(nova,myWriter,random);
	        		if(Mode.equals("W"))
	        			Time =WriterMeasurement(nova,myWriter,random);
	        		Mean.add(Time);
	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		ListOffHeap un=new ListOffHeap(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i,0);

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			ReaderMeasurement(un,myWriter,random);
	        		if(Mode.equals("W"))
	        			WriterMeasurement(un,myWriter,random);	  
	        		}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		if(Mode.equals("R"))
	        			Time =ReaderMeasurement(un,myWriter,random);
	        		if(Mode.equals("W"))
	        			Time =WriterMeasurement(un,myWriter,random);
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

	
	public long Measurement(ListInterface list,  FileWriter myWriter , String Mode , int random) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();
	    
	    for (int i = 0; i < NUM_THREADS-2; i++) {
	    	if( random  == 0) 	    
	    		threads.add(new Thread(new ReaderThreadSerial(latch,list,i,false)));
	    	else
	    		threads.add(new Thread(new ReaderThread(latch,list,i,rng.nextLong(),false)));

	    	threads.get(i).start();
	    	}	 			
    	if(Mode.equals("R")) {
		    	if( random  == 0){
		        	threads.add(new Thread(new WriterThreadSerial(latch,list,NUM_THREADS-2,false)));
		    		threads.add(new Thread(new ReaderThreadSerial(latch,list,NUM_THREADS-1,true)));
		    	}
		    	else {
		        	threads.add(new Thread(new WriterThread(latch,list,NUM_THREADS-2,rng.nextLong(),false)));
		    		threads.add(new Thread(new ReaderThread(latch,list,NUM_THREADS-1,rng.nextLong(),true)));
		    	}
        	}
    	if(Mode.equals("W")) {
		    	if( random  == 0){
		        	threads.add(new Thread(new ReaderThreadSerial(latch,list,NUM_THREADS-2,false)));
		    		threads.add(new Thread(new WriterThreadSerial(latch,list,NUM_THREADS-1,true)));
		    	}
		    	else {
		    		threads.add(new Thread(new ReaderThread(latch,list,NUM_THREADS-2,rng.nextLong(),false)));
		        	threads.add(new Thread(new WriterThread(latch,list,NUM_THREADS-1,rng.nextLong(),true)));
		    	}
    		}
	    threads.get(NUM_THREADS-2).start();
    	threads.get(NUM_THREADS-1).start();
     
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();
	    final long startTime = System.nanoTime();
	    
	    threads.get(NUM_THREADS-1).join();
	    final long endTime = System.nanoTime();

	    stop=true;
	    for (int i = 0; i < NUM_THREADS-1; i++) 
	    	threads.get(i).join();
	    stop=false;
	    if(myWriter!= null) {
	    	myWriter.write((endTime - startTime)+"\n");
	    	}
	    return (endTime - startTime);
	}
	
	public long ReaderMeasurement(ListInterface list, FileWriter file, int random) {
		try {
			return Measurement(list, file, "R" , random);
		}catch(Exception e) {}
		return 0;
	}
	
	public long WriterMeasurement(ListInterface list, FileWriter file, int random) {
		try {
			return Measurement(list, file, "W", random);
		}catch(Exception e) {}
		return 0;
	}	
	
	
	//java -cp target/nova-0.0.1-SNAPSHOT.jar -server com.yahoo.oak.BenchmarkConcurrent List Operation Threads Rand
	
	public static void main(String[] args) throws java.io.IOException {
    	if(args[0]==null) {
    		System.out.print("No args !\n");
    	}
    	String List	=	args[0];
    	String mode	=	args[1];
    	int threads	= 	Integer.parseInt(args[2]);
    	int random  =   Integer.parseInt(args[3]);
    	BenchmarkConcurrent test= new BenchmarkConcurrent();
    	test.ConcurrentReadWriteBenchmark(List,threads,mode,random);
    	
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
	
	
    public class ReaderThreadSerial extends bench_Thread{
    	boolean Measured;
    	ReaderThreadSerial(CountDownLatch latch,ListInterface list,int index, boolean Measured) {
    		super(latch, list, index);
    		this.Measured = Measured;
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            int j=LIST_SIZE/2,i=0;
            if (Measured) {
				while(i<Limit) {
	            	list.get(j,idx);
					j++;
					i++;
	            	if(j == LIST_SIZE)
	            		j = LIST_SIZE/2;
	            	}
            }else {
                while(!stop) {
                	list.get(j,idx);
					j++;
					i++;
	            	if(j == LIST_SIZE)
	            		j = LIST_SIZE/2;
            	}
            }

        }
    }
    
	public class WriterThreadSerial extends bench_Thread{
		boolean Measured;
		WriterThreadSerial(CountDownLatch latch,ListInterface list,int index, boolean Measured) {
			super(latch, list, index);
			this.Measured= Measured;
			}  
		@Override
		public void run() {
			try {
				latch.await();
				} catch (Exception e) {
					e.printStackTrace();
					}
			int j=LIST_SIZE/2,i=0;
			if (Measured) {
				while(i<Limit) {
					list.set(i,i,idx);
					j++;
					if(j == LIST_SIZE/2 + rangeforReadWrite)
						j = LIST_SIZE/2;
					}
				
			}else {
				while(!stop) {
					while(i<Limit) {
						list.set(j, j, idx);
						j++;
						i++;
						if(j == LIST_SIZE/2 + rangeforReadWrite)
							j = LIST_SIZE/2;
						}
	            	}
				}
			}

		}
}
