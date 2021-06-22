package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.yahoo.oak.FacadeTest.ReaderThread;

import sun.misc.Unsafe;

public class longPrimitiveFacade {

	
    private  NovaManager  novaManager;

	private  Facade_Nova facade;
    private  ArrayList<Thread> threads;
    private static CountDownLatch latch = new CountDownLatch(1);

    private  final int NUM_THREADS = 3;
    
    
    protected final static class Node {
        final long key;
        final long value;
        volatile Node  left;
        volatile Node  right;

        /** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
        Node(final long key, final long value,
     		   	final Node left, final Node  right) {
            this.key = key;
            this.value = value;
            this.left = left;
            this.right = right;
        }

        /** TO CREATE A LEAF NODE **/
        Node(final long key, final long value) {
            this(key, value, null, null);
        }

        /** TO CREATE AN INTERNAL NODE **/
        Node(final long key, final Node left, final Node right) {
            this(key, Illegal_facade, left, right);
        }
    }
    
	   static final long Facade_long_offset_key;
	   static final long Facade_long_offset_value;
	   static final long Illegal_facade = 1;
		static {
			try {
				final Unsafe UNSAFE=UnsafeUtils.unsafe;
				Facade_long_offset_key= UNSAFE.objectFieldOffset
					    (Node.class.getDeclaredField("key"));
				Facade_long_offset_value = UNSAFE.objectFieldOffset
						  (Node.class.getDeclaredField("value"));
				 } catch (Exception ex) { throw new Error(ex); }
		}


    private  void initNova() {
        final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
         novaManager = new NovaManager(allocator);
         Facade_Nova.novaManager = novaManager;
        threads = new ArrayList<>(NUM_THREADS);
    }
    
    
	@Test 
	public void concurrentREAD() throws InterruptedException {
		initNova();   
		Node x = new Node(1, 1);
		Facade_Nova.AllocateSlice(x,Facade_long_offset_key, 8, 0);
	}
}
