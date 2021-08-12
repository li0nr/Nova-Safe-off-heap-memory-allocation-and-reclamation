package com.yahoo.oak.LL.IBR;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_HE;
import com.yahoo.oak.HazardEras;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.HazardEras.HEslice;
import com.yahoo.oak.IBR_;
import com.yahoo.oak.IBR_.IBRslice;
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
public class LL_IBR_noCAS_opt<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    final IBR_ IBR;
    final static int MAXTHREADS = 32;
    
	
    static class Node {
        final IBRslice key;
        final IBRslice value;
        final AtomicMarkableReference<Node> next;
               
        Node(IBRslice key, IBRslice value ) {
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

    

    
    
    public LL_IBR_noCAS_opt(NativeMemoryAllocator allocator, NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {
    	
    	IBR = new IBR_(allocator);
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
                Vsr.serialize(value, curr.value.address+curr.value.offset);
            	IBR.end_op(tidx);
                return true;
            } else {
            	IBRslice oKey  = IBR.allocate( Ksr.calculateSize(key));
        		Ksr.serialize(key, oKey.address+oKey.offset);
        		IBRslice oValue  = IBR.allocate( Vsr.calculateSize(value));
        		Vsr.serialize(value, oValue.address+oValue.offset);
        		
        		final Node newNode = new Node(oKey, oValue);
        		
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                	IBR.end_op(tidx);
                    return true;
                }
                else {
                	IBR.fastFree(newNode.key);
                	IBR.fastFree(newNode.value);
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
            	IBR.end_op(tidx);
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
            	IBR.end_op(tidx);
            	IBR.retire(tidx, curr.key);
            	IBR.retire(tidx, curr.value);
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
			                    IBR.retire(tidx, curr.key);
			                    IBR.retire(tidx, curr.value);

			                    curr = succ;
			                    succ = curr.next.get(marked);
			                }
			                IBRslice access = IBR.get_protected(curr.key, tidx);
			                if(access == null && curr != tail)
			                	throw new NovaIllegalAccess();
			                if (curr == tail || Kcm.compareKeys(access.address + access.offset, key) >= 0) {
			                    return new Window(pred, curr);
			                    }
			                pred = curr;			
			                curr = succ;
			                }
        				}
        }catch (NovaIllegalAccess e) {continue CmpFail;}    
    }
        
    
    public <R> R get(K key, NovaR Reader, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
		        Node curr = head.next.getReference();
		        curr.next.get(marked);
		        IBRslice access = IBR.get_protected(curr.key, tidx);
                if(access == null && curr != tail )
                	throw new NovaIllegalAccess();
		        while (curr != tail && Kcm.compareKeys(access.address + access.offset, key) < 0) {
		        	curr = curr.next.getReference();
		            curr.next.get(marked);
		            access = IBR.get_protected(curr.key, tidx);
	                if(access == null && curr != tail)
	                	throw new NovaIllegalAccess();
		        }
		        boolean flag = curr.key != null && Kcm.compareKeys(access.address + access.offset, key) == 0 && !marked[0];
            	IBR.end_op(tidx);
		        access = IBR.get_protected(curr.value, tidx);
		        R obj = null;
		        if (flag)
		        	obj = (R)Reader.apply(access.address+ access.offset);
            	IBR.end_op(tidx);
		        return obj;
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
        }
    

    public boolean contains(K key, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
		        Node curr = head.next.getReference();
		        curr.next.get(marked);
		        IBRslice access = IBR.get_protected(curr.key, tidx);
		        while (curr != tail && Kcm.compareKeys(access.address + access.offset, key) < 0) {
		        	curr = curr.next.getReference();
		            curr.next.get(marked);
		            access = IBR.get_protected(curr.key, tidx);
		            }
		        boolean flag = curr.key != null && Kcm.compareKeys(access.address + access.offset, key) == 0 && !marked[0];
            	IBR.end_op(tidx);
		        return flag;
	        }catch (NovaIllegalAccess e) {continue CmpFail;}
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
    
    public boolean Fill(K key, V value , int tidx) {
        CmpFail: while(true)
        try{
        while (true) {
            final Window window = find(key, tidx);
            // On Harris paper, pred is named left_node and curr is right_node
            final Node pred = window.pred;
            final Node curr = window.curr;
            if (curr.key!= null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) { 
            	return false;
            } else {
            	IBRslice oKey  = IBR.allocate( Ksr.calculateSize(key));
        		Ksr.serialize(key, oKey.address+oKey.offset);
        		IBRslice oValue  = IBR.allocate( Vsr.calculateSize(value));
        		Vsr.serialize(value, oValue.address+oValue.offset);
        		
        		final Node newNode = new Node(oKey, oValue);
        		
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                	IBR.end_op(tidx);
                    return true;
                }
                else {
                	IBR.fastFree(newNode.key);
                	IBR.fastFree(newNode.value);
                }
            }
        }       
	}catch(NovaIllegalAccess e) {continue CmpFail;}
}
    
}