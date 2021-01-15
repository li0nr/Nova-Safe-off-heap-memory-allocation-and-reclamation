package com.yahoo.oak;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;



public class RandomBenchmark {
	
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=10_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static  int Limit = 0;
	static  int rangeforReadWrite=1000; //range for the concurrent test all the 15 reader read from the same 1000section, so does the writer.
	
	static boolean stop=false;

    public RandomBenchmark(){    }

    
	public long ReadWriteGeneric(ListInterface list,String mode,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();

		for (int i = 0; i < NUM_THREADS; i++) {
	        if(mode.equals("W"))threads.add(new Thread(new WriterThread(latch,list,i,rng.nextLong())));
	        if(mode.equals("R"))threads.add(new Thread(new ReaderThread(latch,list,i,rng.nextLong())));
	        
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
    
    
    
      
	public long ReadandWrite(ListInterface list,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();

		
		for (int i = 0; i < NUM_THREADS-1; i++) {
			threads.add(new Thread(new ReaderThreadWithRange(latch,list,i,rng.nextLong())));
	        threads.get(i).start();
	    }	 			
		threads.add(new Thread(new WriterThreadWithRange(latch,list,NUM_THREADS,rng.nextLong())));
        threads.get(NUM_THREADS-1).start();

       
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();

        final long startTime = System.nanoTime();

        threads.get(NUM_THREADS-1).join();
        stop=true;
        for (int i = 0; i < NUM_THREADS-1; i++) {
            threads.get(i).join();
        }
        stop=false;
        
        
        final long endTime = System.nanoTime();
        if(myWriter!= null) {
        	myWriter.write((endTime - startTime)+"\n");
        }
        return (endTime - startTime);
      
	}


    
    public  void ReadWriteBenchmark(String list, String mode, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= lenght;
        NUM_THREADS=threads;
        Limit = LIST_SIZE/NUM_THREADS+1;
        FileWriter myWriter = new FileWriter("rand"+list+"_"+mode+"_"+threads+".txt");
		long Time=0;
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++)
	    			nova.add((long)i,0);

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
	    			un.add((long)i,0);

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
	        						+" mode:"+mode+" thread num:"+threads+ "\n");
	        System.out.println(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" mode:"+mode+" thread num:"+threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
  
    public  void ConcurrentReadWriteBenchmark(String list, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE	= lenght;
        NUM_THREADS	= threads;
        Limit 		= 1_000_000;
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
	        		Time=ReadandWrite(nova,myWriter);
	        	}
	        	for (int j=0; j<5 ; j++) {
	        		Thread.sleep(1000);
	        		Time=ReadandWrite(nova,myWriter);
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
	        						+" thread num:"+threads+ "\n");
	        System.out.println(list+"Mean:"+benchMath.Mean(Mean)+" SE:"+benchMath.StandardError(Mean)
	        						+" thread num:"+threads+ "\n");
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }


    

    public  static void RandBenchmark(String List, String mode, int lenght, int threads)throws java.io.IOException {
    	RandomBenchmark s = new RandomBenchmark();
    	s.ReadWriteBenchmark(List, mode, lenght, threads);
    }
    public  static void ConcurrentRandBenchmark(String List, String mode, int lenght, int threads)throws java.io.IOException {
    	RandomBenchmark s = new RandomBenchmark();
    	s.ConcurrentReadWriteBenchmark(List, lenght, threads);
    }
    
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
    	
    	if(args.length == 5) {
    		lenght = Integer.parseInt(args[3]);
    		ConcurrentRandBenchmark(List,mode,lenght, threads);
    	}else 
    	RandBenchmark(List,mode,lenght,threads);
    	
    }
    
    
    /*******************Runnable Threads*********************************************/
    
    
    
    public class WriterThread extends benchThread{
    	WriterThread(CountDownLatch latch,ListInterface list,int index,long seed) {
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
            	list.set(j,j,idx);
            	i++;
            	}
    		}
    	}
        
	public class WriterThreadWithRange extends benchThread{
		WriterThreadWithRange(CountDownLatch latch,ListInterface list,int index,long seed) {
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
            	list.set(j,j,idx);
            	i++;
            	}
			}
		}

    public class ReaderThread extends benchThread{
    	ReaderThread(CountDownLatch latch,ListInterface list,int index,long seed) {
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
            	list.get(j,idx);
            	i++;
            	}
            }
        }
    
    public class ReaderThreadWithRange extends benchThread{
    	ReaderThreadWithRange(CountDownLatch latch,ListInterface list,int index,long seed) {
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
            while(!stop) {
            	j=random.nextInt(rangeforReadWrite);
            	j+=LIST_SIZE/2;
            	list.get(j,idx);
            	i++;
        	}
        }
    }
    
    public class ReaderThreadWithRangeinfinite extends benchThread{
    	ReaderThreadWithRangeinfinite(CountDownLatch latch,ListInterface list,int index,long seed) {
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
            while(!stop) {
            	j=random.nextInt(rangeforReadWrite);
            	j+=LIST_SIZE/2;
            	list.get(j,idx);
            	i++;
        	}
        }
    }
    
}


