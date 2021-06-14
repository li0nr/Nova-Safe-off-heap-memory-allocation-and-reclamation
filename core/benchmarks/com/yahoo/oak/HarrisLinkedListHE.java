package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.HazardEras.HEslice;

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
public class HarrisLinkedListHE<E> {

    final Node<HEslice> head;
    final Node<HEslice> tail;
    
    final NovaC<E> Cmp;
    final NovaSerializer<E> Srz;
    final HazardEras HE;
    final NativeMemoryAllocator allocator;
    final static int MAXTHREADS = 32;
    
    static class Node<E> {
        final HEslice key;
        final AtomicMarkableReference<Node<HEslice>> next;
               
        Node(HEslice key) {
            this.key = key;
            this.next = new AtomicMarkableReference<Node<HEslice>>(null, false);
        }
    }
    
    // Figure 9.24, page 216
    static class Window<T> {
        public Node<T> pred;
        public Node<T> curr;
        
        Window(Node<T> myPred, Node<T> myCurr) {
            pred = myPred; 
            curr = myCurr;
        }
    }

    

    
    
    public HarrisLinkedListHE(NativeMemoryAllocator allocator,NovaC<E> cmp,	NovaSerializer<E> srz) {
    	HE = new HazardEras(1, MAXTHREADS, allocator);
    	this.allocator = allocator;
		Cmp = cmp; Srz = srz;
    	
        tail = new Node<>(null);
        head = new Node<>(null);
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
    public boolean add(E key, int tidx) {
    	HEslice access  = HE.allocate( Srz.calculateSize(key));
		Srz.serialize(key, access.address+access.offset);
		
		final Node<HEslice> newNode = new Node<>(access);
        while (true) {
            final Window<HEslice> window = find(key, tidx);
            // On Harris paper, pred is named left_node and curr is right_node
            final Node<HEslice> pred = window.pred;
            final Node<HEslice> curr = window.curr;
            if (curr.key!= null && Cmp.compareKeys(curr.key.address + curr.key.offset, key) == 0) { 
            	HE.clear(tidx);
            	HE.retire(tidx, access);
                return false;
            } else {
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                	HE.clear(tidx);
                    return true;
                }
            }
        }       
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
    public boolean remove(E key, int tidx) {
        while (true) {
            final Window<HEslice> window = find(key, tidx);
            // On Harris's paper, "pred" is named "left_node" and the "curr"
            // variable is named "right_node".            
            final Node<HEslice> pred = window.pred;
            final Node<HEslice> curr = window.curr;
            if ( Cmp.compareKeys(curr.key.address + curr.key.offset, key) != 0) {
            	HE.clear(tidx);
                return false;
            } 
            final Node<HEslice> succ = curr.next.getReference();
            // In "The Art of Multiprocessor Programming - 1st edition", 
            // the code shown has attemptMark() but we can't use it, 
            // because attemptMark() returns true if the node
            // is already marked, which is not the desired effect, so we 
            // must use compareAndSet() instead.
            if (!curr.next.compareAndSet(succ, succ, false, true)) {
                continue;
            }
            if(pred.next.compareAndSet(curr, succ, false, false)) {
            	HE.clear(tidx);
            	HE.retire(tidx, curr.key);
                return true;	
            }
        }
    }

    
    /**
     * Inspired by Figure 9.24, page 216 
     * <p>
     * Progress Condition: Lock-Free
     *      
     * @param key
     * @return
     */
    public Window<HEslice> find(E key, int tidx) {
        Node<HEslice> pred = null;
        Node<HEslice> curr = null; 
        Node<HEslice> succ = null;
        boolean[] marked = {false};

        // I think there is a special case for an empty list
        if (head.next.getReference() == tail) {
            return new Window<HEslice>(head, tail);
        }
        
        retry: 
        while (true) {
            pred = head;
            //curr = HE.get_protected(pred.next.getReference(),1,tidx);
            curr = pred.next.getReference();
            while (true) {
               // succ = HE.get_protected(curr.next.get(marked),0,tidx);
            	succ = curr.next.get(marked);
                while (marked[0]) {
                    if (!pred.next.compareAndSet(curr, succ, false, false)) {
                        continue retry;
                    }
                    HE.retire(tidx, curr.key);
                    //UnsafeUtils.unsafe.fullFence();//needed analog to the compare exchange strong in java?
                    //HE.protectEraRelease(1, 0, tidx);
                    curr = succ;
                    //succ = HE.get_protected(curr.next.get(marked),0,tidx);
                    succ = curr.next.get(marked);
                }
                HEslice access = HE.get_protected(curr.key, 0, tidx);
                if (curr == tail || Cmp.compareKeys(access.address + access.offset, key) >= 0) {
                    return new Window<HEslice>(pred, curr);
                }
                pred = curr;
                //HE.protectEraRelease(2, 1, tidx);

                curr = succ;
                //HE.protectEraRelease(1, 0, tidx);
            }
        }
    }


    
    /**
     * Searches for a given key.
     * 
     * Inspired by Figure 9.27, page 218 on "The Art of Multiprocessor Programming".
     * 
     * As soon as we find a matching key we immediately return false/true 
     * depending whether the corresponding node is marked or not. We can do 
     * this because add() will always insert new elements immediately after a
     * non-marked node.
     * <p>
     * Progress Condition: Wait-Free - bounded by the number of nodes 
     * 
     * @param key
     * @return
     */
    public boolean contains(E key, int tidx) {
        boolean[] marked = {false};
        //Node<HEslice> curr = HE.get_protected(head.next.getReference(),01,tidx);
        Node<HEslice> curr = head.next.getReference();
        curr.next.get(marked);
        HEslice access = HE.get_protected(curr.key, 0, tidx);
        while (curr != tail && Cmp.compareKeys(access.address + access.offset, key) < 0) {
            //curr = HE.get_protected(curr.next.getReference(),01,tidx);
        	curr = curr.next.getReference();
            curr.next.get(marked);
            access = HE.get_protected(curr.key, 0, tidx);
        }
        boolean flag = curr.key != null && Cmp.compareKeys(access.address + access.offset, key) == 0 && !marked[0];
        HE.clear(tidx);
        return flag;
    }
    
	
}