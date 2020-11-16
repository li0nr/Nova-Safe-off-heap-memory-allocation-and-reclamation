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
		        System.out.println("finsished init\n");

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
        		OffHeapList un=new OffHeapList();
	    		for (int i=0; i<LIST_SIZE; i++)
	    			un.add((long)i);
		        System.out.println("finsished init\n");

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
    
  

	
    public  static void main(String[] args)throws java.io.IOException {
    	RandomBenchmark s = new RandomBenchmark();
    	//s.RunBenchmark(4, 10, "R", "N");
		  String mode = args[0];
		  int num = Integer.parseInt(args[1]);
		  String list = args[2];
    	s.RunBenchmark(num, 100_000_000, mode, list);
    }
    

}
