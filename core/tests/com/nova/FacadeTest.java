package com.nova;


import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;


import org.junit.Assert;
import org.junit.Test;




public class FacadeTest {
	
    private  NovaManager  novaManager;

	private  long facade;
    private  ArrayList<Thread> threads;
    private static CountDownLatch latch = new CountDownLatch(1);

    private  final int NUM_THREADS = 3;
    


    private  void initNova() {
    	final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	novaManager = new NovaManager(allocator);
        threads = new ArrayList<>(NUM_THREADS);
    }
    
    private void initFacade() {
    	new Facade_Nova(novaManager);
    }
    
    public class CreateAllocateSlice implements Runnable {
        CountDownLatch latch;
        int idx;
        CreateAllocateSlice(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }
        @Override
        public void run() {
        	Facade_Nova.AllocateSlice(12,idx);
            }
        }
    
    public class ReaderThread implements Runnable{
        CountDownLatch latch;
        int idx;
        ReaderThread(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }
        @Override
        public void run() {
        	long result=Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R,facade);	
        }
    }
    


    public class WriterThead implements Runnable{
        CountDownLatch latch;
        int idx;
        WriterThead(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }
        @Override
        public void run() {
        	Facade_Nova.WriteFull(com.nova.List_Nova.DEFAULT_SERIALIZER,(long)0,facade,idx);
        }
    }
    
    public class allocateThreads implements Runnable{
        CountDownLatch latch;
        int idx;
        allocateThreads(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }
        @Override
        public void run() {
        	facade = Facade_Nova.AllocateSlice(8,idx);	
        }
    }
    
    
    public class delteThead implements Runnable{
        CountDownLatch latch;
        int idx;
        delteThead(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }
        @Override
        public void run() {
        	try {
            	Facade_Nova.Delete(idx, facade);
        	}catch(NullPointerException e) {
        		e.printStackTrace();
        	}
        }
    }
    
    @Test
	public void testAllocate() throws InterruptedException {
		initNova();
		initFacade();
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new CreateAllocateSlice(latch,i)));
	        threads.get(i).start();
	    }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
	    Assert.assertEquals(NUM_THREADS*(12+novaManager.HEADER_SIZE), novaManager.allocated());
	}
	
	
	@Test 
	public void concurrentREAD() throws InterruptedException {
		initNova();
		initFacade();
		facade =         	Facade_Nova.AllocateSlice(12,0);

	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new ReaderThread(latch,i)));
	        threads.get(i).start();
	    }
	    
        latch.countDown();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
    }


	@Test 
	public void concurrentReadDelete() throws InterruptedException {
		initNova();
		initFacade();
    	facade = Facade_Nova.AllocateSlice(12,0);
		int i = 0;
	    for ( ;i < NUM_THREADS-1; i++) {
	        threads.add(new Thread(new ReaderThread(latch,i)));
	        threads.get(i).start();
	    }
	    CountDownLatch latch2 = new CountDownLatch(2);
	    threads.add(new Thread(new delteThead(latch,2)));
        threads.get(i).start();
        
        latch.countDown();

        latch2.countDown();
        latch2.countDown();

        for (i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

    }
	
	@Test
	public void concurrentWriteDelete() throws InterruptedException {
		initNova();
		initFacade();
		int i = 0;
        threads.add(new Thread(new allocateThreads(latch,2)));
        threads.get(i).start();
        threads.get(i).join();


        threads.add(new Thread(new WriterThead(latch,0)));
        threads.add(new Thread(new delteThead(latch,1)));


	    for ( i=1;i < NUM_THREADS; i++) {
	        threads.get(i).start();
	    }

        

        latch.countDown();
        for (i = 1; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
	}
	

	@Test 
	public void AllocDeAlloc() throws InterruptedException {
		initNova();
		initFacade();
		facade = Facade_Nova.AllocateSlice(8,0);
		Facade_Nova.Delete(0,facade);
		Facade_Nova.AllocateSlice(8,0);
		
	}
	
	
	@Test 
	public void sequentialinitdelete() throws InterruptedException {
		initNova();
		initFacade();
		facade = Facade_Nova.AllocateSlice(8,0);
		Facade_Nova.Delete(0,facade);
		facade = Facade_Nova.AllocateSlice(8,0);		
	}
	
	
	
	
	@Test 
	public void concurrenallocate() throws InterruptedException {
		initNova();
		initFacade();
		int i = 0;
	    for ( ;i < NUM_THREADS; i++) {
	        threads.add(new Thread(new allocateThreads(latch,i)));
	        threads.get(i).start();
	    }
        
        latch.countDown();
        for (i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
		
	}
	
	//ILLEGAL TEST
	@Test 
	public void concurrentdelete() throws InterruptedException {
		initNova();
		initFacade();
		int i = 0;
        threads.add(new Thread(new allocateThreads(latch,i)));
        threads.get(i).start();

	    for (i=1 ;i < NUM_THREADS; i++) {
		    threads.add(new Thread(new delteThead(latch,i)));
	        threads.get(i).start();
	    }
       
        latch.countDown();
        for (i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
		
	}

	
	@Test 
	public void releaseTest() throws InterruptedException {
		initNova();
		initFacade();
		
		long [] f=new long[5];
		for(int i=0; i< f.length; i++) {
			f[i] = Facade_Nova.AllocateSlice(Long.BYTES, 0);
		}
		for(int i=0; i< f.length; i++)
			Facade_Nova.Delete(0,f[i]);
		
	}
	
	@Test 
	public void ConcurrentAllocateDelete() throws InterruptedException {
		int TestThreads = 4;
		initNova();
		initFacade();
		int i = 0;
        threads.add(new Thread(new allocateThreads(latch,0)));
        threads.get(0).start();
	    threads.add(new Thread(new delteThead(latch,1)));
        threads.get(1).start();
	    threads.add(new Thread(new allocateThreads(latch,2)));
        threads.get(2).start();
	    threads.add(new Thread(new delteThead(latch,3)));
        threads.get(3).start();


        
        latch.countDown();
        for (i = 0; i < TestThreads; i++) {
            threads.get(i).join();
        }
		
	}
	
	@Test 
	public void deleteCheck() throws InterruptedException {
		initNova();
		initFacade();
		facade = Facade_Nova.AllocateSlice(8,0);
		Facade_Nova.Delete(0,facade);
	}
	
	@Test 
	public void checkallMethods() throws InterruptedException {
		initNova();
		initFacade();
		long[] alotofFacades = new long[1024*1024/16];
		facade = Facade_Nova.AllocateSlice(8, 0);
		Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R, facade);
		Facade_Nova.WriteFull(com.nova.List_Nova.DEFAULT_SERIALIZER,(long)5, facade, 0);
		Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R, facade);
		Facade_Nova.WriteFast(com.nova.List_Nova.DEFAULT_SERIALIZER,(long)6, facade, 0);
		Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R, facade);
		Facade_Nova.Delete(0, facade);
		facade = Facade_Nova.AllocateSlice(8, 0);
		Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R, facade);

		
    	for(int i=0; i<20;i++) {
    		alotofFacades[i] = Facade_Nova.AllocateSlice(8, 0);
    		Facade_Nova.WriteFast(com.nova.List_Nova.DEFAULT_SERIALIZER,(long)i, alotofFacades[i], 0);

    		assert 		Facade_Nova.Read(com.nova.List_Nova.DEFAULT_R, alotofFacades[i]) == i;
    	}
    	for(int i=0; i<20;i++) {
    		Facade_Nova.Delete(0, alotofFacades[i]);
    	}
    	for(int i=0; i<20;i++) {
    		alotofFacades[i] = Facade_Nova.AllocateSlice(8, 0);
    	}
    	novaManager.close();
    }
}
