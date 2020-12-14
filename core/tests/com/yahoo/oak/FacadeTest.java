package com.yahoo.oak;



import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;


import org.junit.Assert;
import org.junit.Test;




public class FacadeTest {
	
    private  NovaManager  novaManager;

	private  Facade facade;
    private  ArrayList<Thread> threads;
    private static CountDownLatch latch = new CountDownLatch(1);

    private  final int NUM_THREADS = 3;
    
    FacadeWriteTransformer<Void> f=(ByteBuffer) -> {	
    for(int i=0;i <20; i++) {
    	ByteBuffer.putInt(4,4);
    	}
    return null;
    };

    private  void initNova() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
         novaManager = new NovaManager(allocator);

        threads = new ArrayList<>(NUM_THREADS);
    }
    
    private void initFacade() {
    	facade= new Facade(novaManager);
    	facade.AllocateSlice(8,0);
    	facade.Write(ByteBuffer -> ByteBuffer.putInt(4,3),0);

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
        	Facade f=new Facade(novaManager);
        	f.AllocateSlice(10,idx);
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
        	try {
        		long result=facade.Read(ByteBuffer -> ByteBuffer.getInt(4));	
        	}catch (Exception e) {
        		System.out.print(e.toString());
        	}
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
        	try {
        		facade.Write(f,idx);	
        	}catch (Exception e) {
        		System.out.print(e.toString());
        	}
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
        	try {
        		facade.AllocateSlice(8,idx);	
        	}catch (Exception e) {
        		System.out.print(e.toString());
        	}
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
        	facade.Delete(idx);
            }
    }
    @Test
	public void testAllocate() throws InterruptedException {
		initNova();
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new CreateAllocateSlice(latch,i)));
	        threads.get(i).start();
	    }
        latch.countDown();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
	    Assert.assertEquals(NUM_THREADS*(10+8), novaManager.allocated());
	}
	
	
	@Test 
	public void concurrentREAD() throws InterruptedException {
		initNova();
		initFacade();
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
        threads.add(new Thread(new WriterThead(latch,0)));
        threads.add(new Thread(new delteThead(latch,1)));
        threads.add(new Thread(new allocateThreads(latch,2)));


	    for ( i=0;i < NUM_THREADS; i++) {
	        threads.get(i).start();
	    }

        

        latch.countDown();
        for (i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
		
	}
	

	@Test 
	public void AllocDeAlloc() throws InterruptedException {
		initNova();
		initFacade();
		facade.Delete(0);
		facade.AllocateSlice(8,0);
		
	}
	
	
	@Test 
	public void sequentialinitdelete() throws InterruptedException {
		initNova();
    	facade= new Facade(novaManager);
    	facade.AllocateSlice(8,0);
		facade.Delete(0);
		facade.AllocateSlice(8,0);
		
	}
	
	
	
	
	@Test 
	public void concurrenallocate() throws InterruptedException {
		initNova();
    	facade= new Facade(novaManager);
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
    	facade= new Facade(novaManager);
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
	public void ConcurrentAllocateDelete() throws InterruptedException {
		int TestThreads = 4;
		initNova();
    	facade= new Facade(novaManager);
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
    	facade= new Facade(novaManager);
    	facade.AllocateSlice(8,0);
		facade.Delete(0);
	}

}
