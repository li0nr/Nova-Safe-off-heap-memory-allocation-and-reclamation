package com.yahoo.oak.LL.HE;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_EBR;
import com.yahoo.oak.Facade_HE;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.HazardEras.HEslice;
import com.yahoo.oak.LL.HE.LL_HE_noCAS_opt.Node;

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
public class LL_HE_noCAS<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    
	
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

    

    
    
    public LL_HE_noCAS(NativeMemoryAllocator allocator, NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {

    	Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
    	new Facade_HE(allocator);
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
            if (curr.key!= null && Facade_HE.Compare(key, Kcm, curr.key, tidx) == 0) { 
                Facade_HE.Write(Vsr, value, curr.value, tidx);
                return true;
            } else {
            	HEslice oKey  = Facade_HE.allocate( Ksr.calculateSize(key));
        		Ksr.serialize(key, oKey.address+oKey.offset);
            	HEslice oValue  = Facade_HE.allocate( Vsr.calculateSize(value));
        		Vsr.serialize(value, oValue.address+oValue.offset);
        		
        		final Node newNode = new Node(oKey, oValue);
        		
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                    return true;
                }
                else {
                	Facade_HE.fastFree(newNode.key);
                	Facade_HE.fastFree(newNode.value);
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
            if ( curr.key == null ||  Facade_HE.Compare(key, Kcm, curr.key, tidx)  != 0) {
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
            	Facade_HE.Delete(tidx, curr.key);
            	Facade_HE.Delete(tidx, curr.value);
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
			                	Facade_HE.Delete(tidx, curr.key);
			                	Facade_HE.Delete(tidx, curr.value);

			                    curr = succ;
			                    succ = curr.next.get(marked);
			                }
			                if (curr == tail || Facade_HE.Compare(key, Kcm, curr.key, tidx) >= 0) {
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
		        while (curr != tail && Facade_HE.Compare(key, Kcm, curr.key, tidx) < 0) {
		        	curr = curr.next.getReference();
		            curr.next.get(marked);
		        }
		        boolean flag = curr.key != null && Facade_HE.Compare(key, Kcm, curr.key, tidx) == 0 && !marked[0];
		        R obj = null;
		        if (flag)
		        	obj = (R) Facade_HE.Read(Reader, curr.value, tidx);
		        return obj;
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
        }
    
    public boolean contains(K key, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
		        Node curr = head.next.getReference();
		        curr.next.get(marked);
		        while (curr != tail && Facade_HE.Compare(key, Kcm, curr.key, tidx) < 0) {
		        	curr = curr.next.getReference();
		            curr.next.get(marked);
		        }
		        boolean flag = curr.key != null && Facade_HE.Compare(key, Kcm, curr.key, tidx) == 0 && !marked[0];
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
    
    //********************************************//
    //for filling the benchmarks
    public boolean Fill(K key, V value , int tidx) {
        CmpFail: while(true)
        try{
        while (true) {
            final Window window = find(key, tidx);
            // On Harris paper, pred is named left_node and curr is right_node
            final Node pred = window.pred;
            final Node curr = window.curr;
            if (curr.key!= null && Facade_HE.Compare(key, Kcm, curr.key, tidx) == 0) { 
                return false;
            } else {
            	HEslice oKey  = Facade_HE.allocate( Ksr.calculateSize(key));
        		Ksr.serialize(key, oKey.address+oKey.offset);
            	HEslice oValue  = Facade_HE.allocate( Vsr.calculateSize(value));
        		Vsr.serialize(value, oValue.address+oValue.offset);
        		
        		final Node newNode = new Node(oKey, oValue);
        		
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
                    return true;
                }
                else {
                	Facade_HE.fastFree(newNode.key);
                	Facade_HE.fastFree(newNode.value);
                }
            }
        }       
	}catch(NovaIllegalAccess e) {continue CmpFail;}
}
}