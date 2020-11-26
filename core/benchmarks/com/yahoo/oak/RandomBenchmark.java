package com.yahoo.oak;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.oak.NovaListBenchmark.GenericThreadR;
import com.yahoo.oak.NovaListBenchmark.GenericThreadRW;
import com.yahoo.oak.NovaListBenchmark.GenericThreadW;

public class RandomBenchmark {
	
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=10_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static int Limit = 0;
	static int rangeforReadWrite=1000;

    public RandomBenchmark(){    }

    
	public long ReadWriteGeneric(ListInterface list,String s,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    AtomicInteger index= new AtomicInteger(0);
	    Random rng = new Random();
		
		for (int i = 0; i < NUM_THREADS; i++) {
	        if(s.equals("W"))threads.add(new Thread(new GenericThreadW(latch,list,index,rng.nextLong())));
	        if(s.equals("R"))threads.add(new Thread(new GenericThreadR(latch,list,index,rng.nextLong())));
	        if(s.equals("RW"))threads.add(new Thread(new GenericThreadRW(latch,list,index,rng.nextLong())));

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
    //    System.out.println((endTime - startTime));
        return (endTime - startTime);
      
	}
    
    public class GenericThreadW extends benchThread{
    	GenericThreadW(CountDownLatch latch,ListInterface list,AtomicInteger index,long seed) {
    		super(latch, list, index,seed);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            int j=0,i=0;
            while(i<Limit) {
            	j=random.nextInt(LIST_SIZE);
            	list.set(j,2);
            	i++;
        	}
        }

    }
    public class GenericThreadR extends benchThread{
    	GenericThreadR(CountDownLatch latch,ListInterface list,AtomicInteger index,long seed) {
    		super(latch, list, index,seed);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            int j=0,i=0;
            while(i<Limit) {
            	j=random.nextInt(LIST_SIZE);
            list.get(j);
            	i++;
        	}
        }

    }
    public class GenericThreadRW extends benchThread{
    	GenericThreadRW(CountDownLatch latch,ListInterface list,AtomicInteger index,long seed) {
    		super(latch, list, index,seed);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            int j=0,i=0;
            while(i<Limit) {
            	j=random.nextInt(LIST_SIZE);
            	list.set(j, list.get(j)*2);
            	i++;
        	}
        }

    }
      
	public long ReadandWrite(ListInterface list,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    AtomicInteger index= new AtomicInteger(0);
	    Random rng = new Random();

		
		for (int i = 0; i < NUM_THREADS-1; i++) {
			threads.add(new Thread(new ReaderThread(latch,list,index,rng.nextLong())));
	        threads.get(i).start();
	    }	 			
		threads.add(new Thread(new WriterThread(latch,list,index,rng.nextLong())));
        threads.get(NUM_THREADS-1).start();

       
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
    //    System.out.println((endTime - startTime));
        return (endTime - startTime);
      
	}
    public class ReaderThread extends benchThread{
    	ReaderThread(CountDownLatch latch,ListInterface list,AtomicInteger index,long seed) {
    		super(latch, list, index,seed);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            int j=0,i=0;
            while(i<Limit) {
            	j=random.nextInt(rangeforReadWrite);
            	j+=LIST_SIZE/2;
            	list.get(j);
            	i++;
        	}
        }

    }
    
    public class WriterThread extends benchThread{
    	WriterThread(CountDownLatch latch,ListInterface list,AtomicInteger index,long seed) {
    		super(latch, list, index,seed);
    	}  
        @Override
        public void run() {
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            int j=0,i=0;
            while(i<Limit) {
            	j=random.nextInt(rangeforReadWrite);
            	j+=LIST_SIZE/2;
            	list.set(j,3);
            	i++;
        	}
        }

    }
    
    public  void RunBenchmark(int Threads, int items,String mode,String list)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= items;
        NUM_THREADS=Threads;
        Limit = LIST_SIZE/NUM_THREADS+1;
        FileWriter myWriter = new FileWriter("rand"+list+"_"+mode+"_"+Threads+".txt");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i);

	    		for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( nova,mode,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( nova,mode,myWriter);
	                Mean.add(Time);
	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		OffHeapList un=new OffHeapList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i);

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( un,mode,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadWriteGeneric( un,mode,myWriter);
	                Mean.add(Time);
	        	}
        		un.close();
	        }
	        myWriter.write(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" mode:"+mode+" thread num:"+Threads+ "\n");
	        System.out.println(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" mode:"+mode+" thread num:"+Threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
  
    public  void ReadWriteBenchmark(int Threads, int items,String list)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= items;
        NUM_THREADS=Threads;
        Limit = LIST_SIZE/NUM_THREADS+1;
        FileWriter myWriter = new FileWriter("rand read and wirte"+".txt");
        System.out.println("concurrent Read Write benchmark\n");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i);

	    		for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadandWrite( nova,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadandWrite( nova,myWriter);
	                Mean.add(Time);
	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		OffHeapList un=new OffHeapList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i);

	        	for (int j=0; j<3 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadandWrite( un,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadandWrite(un,myWriter);
	                Mean.add(Time);
	        	}
        		un.close();
	        }
	        myWriter.write(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" thread num:"+Threads+ "\n");
	        System.out.println(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" thread num:"+Threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }

	
    public  static void main(String[] args)throws java.io.IOException {
    	RandomBenchmark s = new RandomBenchmark();
    	String mode=null;
    	String list=null;
    	int threads = -1;
    	int benchmark=0;
    	int listSize=1000;
    	
    	if (args.length > 1) 
    		benchmark =Integer.parseInt(args[1]);
    	if (args.length > 4) 
			  mode = args[4];
    	if (args.length > 3) 
    		listSize =  Integer.parseInt(args[3]);
    	if (args.length > 2) 
    		threads = Integer.parseInt(args[2]);
    	if (args.length > 0) 
    		list = args[0];
    	
    	if(benchmark==0)
    		s.RunBenchmark(threads, 1000, mode, list);
    	else 
    		s.ReadWriteBenchmark(threads, listSize, list);

    }
    

}
