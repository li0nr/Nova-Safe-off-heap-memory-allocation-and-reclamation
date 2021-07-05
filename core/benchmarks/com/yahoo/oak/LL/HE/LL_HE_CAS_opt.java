package com.yahoo.oak.LL.HE;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.HazardEras;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaS;
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
public class LL_HE_CAS_opt<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    final HazardEras HE;
    final static int MAXTHREADS = 32;
    
	
    static class Node {
        final HEslice key;
        final HEslice value;
        final AtomicMarkableReference<Node> next;
               
        Node(HEslice key, HEslice value ) {
            this.key = key;
            this.value = value;
            this.next = new AtomicMarkableReference<Node>(null, false);
        }
        
        public Node getNext() {
        	return this.next.getReference();
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

    

    
    
    public LL_HE_CAS_opt(NativeMemoryAllocator allocator, NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {
    	
    	HE = new HazardEras(allocator);
    	Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
    	
        tail = new Node(null, null);
        head = new Node(null, null);
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
    public boolean add(K key, V value , int tidx) {
        CmpFail: while(true)
        try{
        while (true) {
            final Window window = find(key, tidx);
            // On Harris paper, pred is named left_node and curr is right_node
            final Node pred = window.pred;
            final Node curr = window.curr;
            if (curr.key!= null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) { 
            	HE.clear(tidx);
                return false;
            } else {
            	HEslice oKey  = HE.allocateCAS( Ksr.calculateSize(key));
        		Ksr.serialize(key, oKey.address+oKey.offset);
            	HEslice oValue  = HE.allocateCAS( Vsr.calculateSize(value));
        		Vsr.serialize(value, oValue.address+oValue.offset);
        		
        		final Node newNode = new Node(oKey, oValue);
        		
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                	HE.clear(tidx);
                    return true;
                }
                else {
                	HE.fastFree(newNode.key);
                	HE.fastFree(newNode.value);
                }
            }
        }       
	}catch(NovaIllegalAccess e) {continue CmpFail;}
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
    public boolean remove(K key, int tidx) {
        CmpFail: while(true)
        try{
    	while (true) {
            final Window window = find(key, tidx);
            // On Harris's paper, "pred" is named "left_node" and the "curr"
            // variable is named "right_node".            
            final Node pred = window.pred;
            final Node curr = window.curr;
            if ( curr.key == null ||Kcm.compareKeys(curr.key.address + curr.key.offset, key) != 0) {
            	HE.clear(tidx);
                return false;
            } 
            final Node succ = curr.next.getReference();
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
            	HE.retireCAS(tidx, curr.key);
            	HE.retireCAS(tidx, curr.value);
                return true;	
                }
            }
    	}catch(NovaIllegalAccess e) {continue CmpFail;}
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
			                    HE.retireCAS(tidx, curr.key);
			                    HE.retireCAS(tidx, curr.value);

			                    curr = succ;
			                    succ = curr.next.get(marked);
			                }
			                HEslice access = HE.get_protected(curr.key, tidx);
			                if (curr == tail || Kcm.compareKeys(access.address + access.offset, key) >= 0) {
			                    return new Window(pred, curr);
			                    }
			                pred = curr;			
			                curr = succ;
			                }
        				}
        }catch (NovaIllegalAccess e) {continue CmpFail;}    
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
    public boolean contains(K key, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
		        Node curr = head.next.getReference();
		        curr.next.get(marked);
		        HEslice access = HE.get_protected(curr.key, tidx);
		        while (curr != tail && Kcm.compareKeys(access.address + access.offset, key) < 0) {
		        	curr = curr.next.getReference();
		            curr.next.get(marked);
		            access = HE.get_protected(curr.key, tidx);
		        }
		        boolean flag = curr.key != null && Kcm.compareKeys(access.address + access.offset, key) == 0 && !marked[0];
		        HE.clear(tidx);
		        return flag;
	        }catch (NovaIllegalAccess e) {continue CmpFail;}
        }
    
    
	public HazardEras getHE() {
		return HE;
	}
}