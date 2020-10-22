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
    	facade.AllocateSlice(8);
    	facade.Write(ByteBuffer -> ByteBuffer.putInt(4,3));

    }
    
    public class RunThreads implements Runnable {
        CountDownLatch latch;

        RunThreads(CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void run() {
        	Facade f=new Facade(novaManager);
        	f.AllocateSlice(10);
            }
        }
    
    public class ReadTheads implements Runnable{
        CountDownLatch latch;

        ReadTheads(CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void run() {
        	try {
//        		int result=facade.Read(ByteBuffer -> ByteBuffer.getInt(4));	
        	}catch (Exception e) {
        		System.out.print(e.toString());
        	}
        }
    }
    


    public class WriteTheads implements Runnable{
        CountDownLatch latch;

        WriteTheads(CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void run() {
        	try {
        		facade.Write(f);	
        	}catch (Exception e) {
        		System.out.print(e.toString());
        	}
        }
    }
    
    
    public class delteThead implements Runnable{
        CountDownLatch latch;

        delteThead(CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void run() {
        	facade.Delete();
            }
    }
    @Test
	public void testAllocate() throws InterruptedException {
		initNova();
	    for (int i = 0; i < NUM_THREADS; i++) {
	        threads.add(new Thread(new RunThreads(latch)));
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
	        threads.add(new Thread(new ReadTheads(latch)));
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
	        threads.add(new Thread(new ReadTheads(latch)));
	        threads.get(i).start();
	    }
	    CountDownLatch latch2 = new CountDownLatch(2);
	    threads.add(new Thread(new delteThead(latch2)));
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
	    for ( ;i < NUM_THREADS-1; i++) {
	        threads.add(new Thread(new WriteTheads(latch)));
	        //threads.get(i).setPriority(6);
	        threads.get(i).start();
	    }
	    CountDownLatch latch2 = new CountDownLatch(2);
	   // threads.add(new Thread(new delteThead(latch)));
//	    threads.get(i).setPriority(9);
//	    threads.get(i).start();
        
        latch.countDown();
        for (i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }
		
	}
	

	@Test 
	public void AllocDeAlloc() throws InterruptedException {
		initNova();
		initFacade();
		facade.Delete();
		facade.AllocateSlice(8);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
    @Test(expected =  IllegalArgumentException.class)
    public void SanitySimple() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(128);
        NovaManager novaManager = new NovaManager(allocator);
        Facade f=new Facade(novaManager);
        f.AllocateSlice(20);
        f.LocateSlice();
        
//        int a=f.Read( ByteBuffer -> ByteBuffer.getInt(4));
//        Object b=f.Write(ByteBuffer -> ByteBuffer.putInt(4, 3));
//        a=f.Read( ByteBuffer -> ByteBuffer.getInt(4));
//        b=f.Write( ByteBuffer -> ByteBuffer.putInt(4,ByteBuffer.getInt(4)*3));
//        a=f.Read( ByteBuffer -> ByteBuffer.getInt(4));
//
//        b=f.Write(this.f);
//
//        a=f.Read( ByteBuffer -> ByteBuffer.getInt(4));

        Facade r=f;
        f.Delete();
        f.Read( ByteBuffer ->ByteBuffer.getInt(4));

    }
        
        
        
        
        
        
        
        
        
   

}
