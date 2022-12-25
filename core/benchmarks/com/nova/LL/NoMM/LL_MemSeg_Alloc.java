package com.nova.LL.NoMM;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.nova.Facade_Nova;
import com.nova.Facade_Slice;
import com.nova.MemorySegmentAllocator;
import com.nova.Facade_Slice.Facade_slice;
import com.nova.NovaC;
import com.nova.NovaIllegalAccess;
import com.nova.NovaManager;
import com.nova.NovaR;
import com.nova.NovaS;
import com.nova.UnsafeUtils;
import com.nova.Buff.Buff.MemSegmentReader;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

/**
 * <h1>HarrisAMRLinkedList</h1>
 * Harris's Linked List with AtomicMarkableReference.
 * <p>
 * This set has three operations:
 * <ul>
 * <li>add(x)      - Lock-Free
 * <li>remove(x)   - Lock-Free
 * <li>contains(x) - Wait-Free
 * </ul><p>
 * Lock-Free Linked List as described in Harris's paper:
 * {@link http://research.microsoft.com/pubs/67089/2001-disc.pdf}
 * <a href="http://research.microsoft.com/pubs/67089/2001-disc.pdf">Harris's paper</a>
 * <p>
 * This is based on the implementation that uses AtomicMarkableReference and 
 * with contains() that does not help add/remove, as described on the book
 * "The Art of Multiprocessor Programming". See figures 9.24, 9.25, 9.26, 9.27 
 *
 * <p>
 * Memory usage per key: One "Node" object with two pointers, one 
 * AtomicMarkableReference instance with one reference, one Pair instance with 
 * one reference and one boolean. 
 * 32 bit machine: 4 x (2+2 + 1+2 + 2+2) = 44 bytes per key
 * 64 bit machine: 8 x (2+2 + 1+2 + 2+2) = 88 bytes per key
 * Notice that Objects in Java use at least 2 words.
 * <p>
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
public class LL_MemSeg_Alloc<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    final MemorySegmentAllocator allocator;
    
    final static int MAXTHREADS = 32;
    final static int Illegal_nu = 1;

    
    static class Node {
        final MemorySegment key;
        final MemorySegment value;
        final AtomicMarkableReference<Node> next;
               
        Node(MemorySegment key, MemorySegment value) {
            this.key = key;
            this.value = value;
            this.next = new AtomicMarkableReference<Node>(null, false);
        }
    }
    
    // Figure 9.24, page 216
    static class Window {
        public Node pred;
        public Node curr;
        
        Window(Node myPred, Node myCurr) {
            pred = myPred; 
            curr = myCurr;
        }
    }
    
    
    public LL_MemSeg_Alloc(MemorySegmentAllocator allocator
    		,NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {	
		this.allocator = allocator; Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
      
        tail = new Node(null,null);
        head = new Node(null,null);
        head.next.set(tail, false);
    }
    
    
    /**
     * Taken from Figure 9.25, page 217
     * For more info take a look at Fig 2 from Harris's paper
     * <p>
     * Progress Condition: Lock-Free
     * 
     * @param key
     * @return
     */
    public boolean add(K key, V value,  int idx) {
    	

        CmpFail: while(true)
        try{
        	while (true) {
                final Window window = find(key, idx);
                // On Harris paper, pred is named left_node and curr is right_node
                final Node pred = window.pred;
                final Node curr = window.curr;
	            if (curr.key != null && Kcm.compareKeys(curr.key, key) == 0) {
	                //Vsr.serialize(value,curr.value.address + curr.value.offset);
	    			ResourceScope.Handle segmentHandle = curr.value.scope().acquire();
	    			try {
	    				MemoryAccess.setIntAtOffset(curr.value, 4, ~MemoryAccess.getIntAtOffset(curr.value,4));
	    			} finally {
	    				curr.value.scope().release(segmentHandle);
	    			}
	            	return true;
	            } else {
	    	    	MemorySegment myK = allocator.allocate(Ksr.calculateSize(key));
	    	    	MemorySegment myV = allocator.allocate(Vsr.calculateSize(value));

	    			Ksr.serialize(key, myK);
	    			Vsr.serialize(value, myV);
	    			
	    			final Node newNode = new Node(myK, myV);
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                    return true;
	                }
                    else {
                    	allocator.free(newNode.key);
                    	allocator.free(newNode.value);
                    }	
	            }
	        }   
        }catch(IllegalStateException e) {continue CmpFail;}
    }
    
    /**
     * Inspired by Figure 9.26, page 218 on "The Art of Multiprocessor Programming".
     * For more info take a look at Fig 3 from Harris's paper.
     * <p>
     * Progress Condition: Lock-Free
     * 
     * @param key
     * @return
     */
    public boolean remove(K key, int idx) {
        CmpFail: while(true)
    	try {
            while (true) {
                final Window window = find(key, idx);
                // On Harris's paper, "pred" is named "left_node" and the "curr"
                // variable is named "right_node".            
                final Node pred = window.pred;
                final Node curr = window.curr;
               
                if (curr.key == null ||  Kcm.compareKeys(curr.key, key) != 0) {
                    return false;
                } 
                final Node succ = curr.next.getReference();
                // In "The Art of Multiprocessor Programming - 1st edition", 
                // the code shown has attemptMark() but we can't use it, 
                // because attemptMark() returns true if the node
                // is already marked, which is not the desired effect, so we 
                // must use compareAndSet() instead.
                if (!curr.next.compareAndSet(succ, succ, false, true)) {//mark
                    continue;
                }
                if(pred.next.compareAndSet(curr, succ, false, false)) {
                	allocator.free(curr.key);
                	allocator.free(curr.value);
                    return true;                	
                }
            }
    	}catch(IllegalStateException e) {continue CmpFail;}
    }

    
    /**
     * Inspired by Figure 9.24, page 216 
     * <p>
     * Progress Condition: Lock-Free
     *      
     * @param key
     * @return
     */
    public Window find(K key, int tidx) {
        Node pred = null;
        Node curr = null; 
        Node succ = null;
        boolean[] marked = {false};

        // I think there is a special case for an empty list
        if (head.next.getReference() == tail) {
            return new Window(head, tail);
        }
        CmpFail: while(true)
        	try {
        		retry: 
        			while (true) {
        				pred = head;
	                    curr = pred.next.getReference();
	                    while (true) {
	                        succ = curr.next.get(marked);
	                        while (marked[0]) {
	                            if (!pred.next.compareAndSet(curr, succ, false, false)) {
	                                continue retry;
	                            }
	                        	allocator.free(curr.key);
	                        	allocator.free(curr.value);
	                            curr = succ;
	                            succ = curr.next.get(marked);
	                        }
	                        if (curr == tail ||  Kcm.compareKeys(curr.key, key) >= 0) { //we compare the offheap vs the key thus looking for >
	                            return new Window(pred, curr);
	                        }
	                        pred = curr;
	                        curr = succ;
	                    }
        			}
        }catch (IllegalStateException e) {continue CmpFail;}
    }



	public <R>R get(K key, MemSegmentReader Reader, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
                Node curr = head.next.getReference();
                curr.next.get(marked);
                while (curr != tail && Kcm.compareKeys(curr.key, key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                boolean flag =  curr.key == null ? false : Kcm.compareKeys(curr.key, key) == 0 && !marked[0];
                R obj = null;
                if(flag) 
                	obj = (R) Reader.apply(curr.value);
                return obj;
        	}catch (IllegalStateException e) {continue CmpFail;}
    }
    
    public boolean contains(K key, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
                Node curr = head.next.getReference();
                curr.next.get(marked);
                while (curr != tail && Kcm.compareKeys(curr.key, key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                return curr.key == null ? false : Kcm.compareKeys(curr.key, key) == 0 && !marked[0];
        	}catch (IllegalStateException e) {continue CmpFail;}
    }
    
    
    public boolean putIfAbsentOak(K key, V value,  int idx) {
        CmpFail: while(true)
        try{
        	while (true) {
                final Window window = find(key, idx);
                // On Harris paper, pred is named left_node and curr is right_node
                final Node pred = window.pred;
                final Node curr = window.curr;
                if (curr.key!= null && Kcm.compareKeys(curr.key, key) == 0) { 
                    return false;
                } else {
                    
                	
	    	    	MemorySegment myK = allocator.allocate(Ksr.calculateSize(key));
	    	    	MemorySegment myV = allocator.allocate(Vsr.calculateSize(value));

	    			Ksr.serialize(key, myK);
	    			Vsr.serialize(value, myV);
                    
                	Node newNode = new Node(myK, myV);

                    newNode.next.set(curr, false);
                    if (pred.next.compareAndSet(curr, newNode, false, false)) {
                        return true;
                    }
                    else {
                    	allocator.free(newNode.key);
                    	allocator.free(newNode.value);
                    }
                }
            }  
        }catch(IllegalStateException e) {continue CmpFail;}
    }
    
    public int Size() {
    	int i = 0;
        Node curr = head.next.getReference();
        while (curr != tail ) {
           curr = curr.next.getReference();
           i ++;
        }
        return i;
    }
}