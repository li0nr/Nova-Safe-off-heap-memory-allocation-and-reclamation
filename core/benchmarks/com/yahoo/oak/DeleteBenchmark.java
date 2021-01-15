package com.yahoo.oak;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;


import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

public class DeleteBenchmark {
	
	
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=10_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static  int Limit = 0;
	static  int rangeforReadWrite=1000;
	
	static int minutes3= 3*60*1000;
	static volatile boolean stop=false;

    public DeleteBenchmark(){    }

    
	public long DeleteWrite(ListInterface list,  FileWriter myWriter) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();

		for (int i = 0; i < NUM_THREADS; i++) {
			threads.add(new Thread(new DeleteWriteThread(latch,list,i,rng.nextLong())));
	        
	        threads.get(i).start();
	    }	   
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();

        
        Thread.sleep(minutes3);
        log_mem(list,myWriter);
        Thread.sleep(minutes3);
        log_mem(list,myWriter);
        Thread.sleep(minutes3);
        log_mem(list,myWriter);
        
        stop =true;

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
        
        final long endTime = System.nanoTime();
        return 0;
      
	}
    


    
    public  void ReadWriteBenchmark(String list, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        LIST_SIZE= lenght;
        NUM_THREADS=threads;
        Limit = LIST_SIZE/NUM_THREADS; //operation to do per thread
        rangeforReadWrite= LIST_SIZE/NUM_THREADS; // the range accessed by each thread
        FileWriter myWriter = new FileWriter("WD"+list+"_"+threads+".txt");
		long Time=0;
		long MemUsed=0;
		long Memallocated=0;
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++) {
	    			nova.add((long)i,0);
	    			if(i%2 ==0)
    				nova.remove(i, 0);
	    		}
	    		log_mem(nova, myWriter);
            	myWriter.write("**********warmup*********\n");
	    		for (int j=0; j<1 ; j++) {
	        		Thread.sleep(1000);
	        		Time=DeleteWrite( nova,myWriter);
	        	}
            	myWriter.write("**********iteration*********\n");
	        	for (int j=0; j<2 ; j++) {
	        		Thread.sleep(1000);
	        		Time=DeleteWrite( nova,myWriter);
	            	myWriter.write("**********"+j+"*********\n");

	        	}
                nova.close();
	        }
	        if(list.equals("U")) {//un-man
        		OffHeapList un=new OffHeapList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++) {
	    			un.add((long)i,0);
	    			if(i%2 ==0)
    				un.delete(i,0);
	    		}
            	myWriter.write("**********warmup*********\n");
	        	for (int j=0; j<1 ; j++) {
	        		Thread.sleep(1000);
	        		Time=DeleteWrite( un,myWriter);
	        	}
            	myWriter.write("**********iteration*********\n");
	        	for (int j=0; j<2 ; j++) {
	        		Thread.sleep(1000);
	        		Time=DeleteWrite( un,myWriter);
	            	myWriter.write("**********"+j+"*********\n");
	        		}
	        	un.close();
	        }
	        myWriter.close();
	        System.gc();
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    


    

    public  static void ConcurrentDeleteWriteBenchmark(String List, int lenght, int threads)throws java.io.IOException {
    	DeleteBenchmark s = new DeleteBenchmark();
    	s.ReadWriteBenchmark(List,  lenght, threads);
    }

    
    public static void main(String[] args)throws java.io.IOException {
    	int lenght = 1000;
    	if(args[0]==null) {
    		System.out.print("No args !\n");
    	}
    	String List	=	args[0];
    	int threads	= 	Integer.parseInt(args[1]);
    	if(args.length == 3) 
    		lenght 	= 	Integer.parseInt(args[2]);

    	ConcurrentDeleteWriteBenchmark(List,lenght,threads);
    	
    }
    
    
    
    public void log_mem(ListInterface L,FileWriter myWriter) {
    	try {
    	myWriter.write("used mem: "+L.getUsedMem()+"\n");
    	myWriter.write("allocated mem: "+L.getAllocatedMem()+"\n");
    	}catch(Exception e) {}
    }
    
    /*******************Runnable Threads*********************************************/
    
        
	public class DeleteWriteThread extends benchThread{
		int StartPoint;
		DeleteWriteThread(CountDownLatch latch,ListInterface list,int index,long seed) {
			super(latch, list, index,seed);
			StartPoint = index*LIST_SIZE/NUM_THREADS;
			}  
		@Override
		public void run() {
			try {
				latch.await();
				} catch (Exception e) {
					e.printStackTrace();
					}
			int i=0,j=0;
			while(!stop) {
				j=StartPoint;
				if(i%2==0) {
					while(j<StartPoint+Limit-2) {
						j++;
						list.delete(j, idx);
						j++;
						list.allocate(j, idx);
						list.set(j, j, idx);

					}
				}
				else
					while(j<StartPoint+Limit-2) {
						j++;
						list.allocate(j, idx);
						list.set(j, j, idx);
						j++;
						list.delete(j, idx);

						}
				i++;
            	}
			}
		}

}
