package com.yahoo.oak;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.experimental.theories.FromDataPoints;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;

public class BenchmarkDelete {
	
	
	static  int NUM_THREADS=1;
	static  int LIST_SIZE=5_000_000;
	static  int RUNS= 10;
	static  int Section = 8;//128 cache line /16 nova number  
	static  int Limit = 0;
	static  int rangeforReadWrite=1000;
	
	static int SleepTime=5*60*1000;
	static volatile boolean stop=false;
	
	static final String scriptpath= "../../benchmarks";

    public BenchmarkDelete(){    }

    
	public long DeleteWrite(ListInterface list,  String myWriter, String List) throws InterruptedException, IOException{
		CountDownLatch latch = new CountDownLatch(NUM_THREADS);
	    ArrayList<Thread> threads = new ArrayList<>();
	    Random rng = new Random();

		for (int i = 0; i < NUM_THREADS; i++) {
			threads.add(new Thread(new DeleteWriteThread(latch,list,i,rng.nextLong())));
	        
	        threads.get(i).start();
	    }	   
	    for (int i=0; i<=NUM_THREADS; i++)
	    	latch.countDown();
	    
        log_mem(list,myWriter,List);
        Thread.sleep(SleepTime);
        log_mem(list,myWriter,List);
        Thread.sleep(SleepTime);
        log_mem(list,myWriter,List);
        Thread.sleep(SleepTime);
        log_mem(list,myWriter,List);
        Thread.sleep(SleepTime);
        log_mem(list,myWriter,List);
        stop =true;

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
                return 0;
      
	}
    


    
    public  void DeleteBenchmark(String list, int lenght, int threads)throws java.io.IOException {
        ArrayList<Long>	 Mean = new ArrayList<>();
        NUM_THREADS=threads;
        Limit = LIST_SIZE/NUM_THREADS; //operation to do per thread
        rangeforReadWrite= LIST_SIZE/NUM_THREADS; // the range accessed by each thread
        String  myWriter = "WD"+list+"_"+threads+".txt";
		try {
	        if(list.equals("N")) {//nova 
        		NovaList nova=new NovaList(LIST_SIZE);
	    		for (int i=0; i<LIST_SIZE; i++) {
	    			nova.add((long)i,0);
	    			if(i%2 ==0)
    				nova.remove(i, 0);
	    		}
	        	for (int j=0; j<1 ; j++) {
	        		Thread.sleep(1000);
	        		DeleteWrite( nova,myWriter,"N");

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
	        	for (int j=0; j<1 ; j++) {
	        		Thread.sleep(1000);
	        		DeleteWrite( un,myWriter,"U");
	        		}
	        	un.close();
	        }
		}catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    


    

    public  static void ConcurrentDeleteWriteBenchmark(String List, int lenght, int threads)throws java.io.IOException {
    	BenchmarkDelete s = new BenchmarkDelete();
    	s.DeleteBenchmark(List,  lenght, threads);
    }

    
    
    // java -XX:NativeMemoryTracking=summary com.yahoo.oak.BenchmarkDelete List threads List_size
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
    
    
    
    public void log_mem(ListInterface L,String myWriter, String List) {
	    //"/bin/bash"
    	final  long GB = 1024*1024*1024;
    	try {
            float heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
            float heapFreeSize = Runtime.getRuntime().freeMemory();
            float allocated = Float.NaN;
            
            System.out.format("      Heap Total: %.4f GB\n",  heapSize / GB);
            System.out.format("      Heap Usage: %.4f GB\n", (heapSize - heapFreeSize) / GB);
    	
            //to get approximate for directalloc
    	//    Process p = new ProcessBuilder("/bin/sh",scriptpath+"/MemLog.sh", myWriter,"2>Error.txt").start();
    	    Process p = new ProcessBuilder("cmd.exe", "/c","wsl",scriptpath+"/MemLog.sh", myWriter,"2>Error.txt").start();

    	  
            p.waitFor();  
    	} catch(Exception e) {}

    }
    
    /*******************Runnable Threads*********************************************/
    
        
	public class DeleteWriteThread extends bench_Thread{
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



//List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
//
//for (BufferPoolMXBean pool : pools) {
//    System.out.println(pool.getName());
//    System.out.println(pool.getCount());
//    System.out.println("memory used " + pool.getMemoryUsed());
//    System.out.println("total capacity" + pool.getTotalCapacity());
//    System.out.println();
//}
