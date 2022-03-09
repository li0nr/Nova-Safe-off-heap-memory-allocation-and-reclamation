package com.yahoo.oak.LL.Nova;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;

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
public class LL_Nova_noCAS<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    final NovaManager nm;
    
    final static int MAXTHREADS = 32;
    final static int Illegal_nu = 1;

    
    static class Node {
        final Facade_slice key;
        final Facade_slice value;
        final AtomicMarkableReference<Node> next;
               
        Node(Facade_slice key, Facade_slice value) {
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
    
    
    public LL_Nova_noCAS(NovaManager novaManager,NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {	
		nm = novaManager; Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
		new Facade_Slice(nm);
	      
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
                if (curr.key!= null && Facade_Slice.Compare(key, Kcm, curr.key) == 0) { 
                    //Facade_Slice.WriteFull(Vsr, value, curr.value, idx);
                	Facade_Slice.OverWrite( (val_addr)-> {
            				UnsafeUtils.putInt(val_addr+4,~UnsafeUtils.getInt(val_addr+4));//4 for capacity
            					return val_addr;	
            			},curr.value,idx);
                	return true;
                } else {
                    
                	Node newNode = new Node(new Facade_slice(), new Facade_slice());
                	
					Facade_Slice.AllocateSlice(newNode.key,Ksr.calculateSize(key), idx);
					Facade_Slice.AllocateSlice(newNode.value,Vsr.calculateSize(value), idx);
                    	
					Facade_Slice.WriteFast(Ksr, key, newNode.key, idx);
					Facade_Slice.WriteFast(Vsr, value, newNode.value, idx);
                    
                    newNode.next.set(curr, false);
                    if (pred.next.compareAndSet(curr, newNode, false, false)) {
                        return true;
                    }
                    else {
                    	Facade_Slice.DeletePrivate(idx, newNode.key);
                    	Facade_Slice.DeletePrivate(idx, newNode.value);
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
    public boolean remove(K key, int idx) {
        CmpFail: while(true)
    	try {
            while (true) {
                final Window window = find(key, idx);
                // On Harris's paper, "pred" is named "left_node" and the "curr"
                // variable is named "right_node".            
                final Node pred = window.pred;
                final Node curr = window.curr;
                if (curr.key == null || Facade_Slice.Compare(key, Kcm, curr.key) != 0) {
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
                	Facade_Slice.Delete(idx, curr.key);
                	Facade_Slice.Delete(idx, curr.value);
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
	                        	Facade_Slice.Delete(tidx, curr.key);
	                        	Facade_Slice.Delete(tidx, curr.value);
	                            curr = succ;
	                            succ = curr.next.get(marked);
	                        }
	
	                        if (curr == tail || Facade_Slice.Compare(key, Kcm, curr.key) >= 0) { //we compare the offheap vs the key thus looking for >
	                            return new Window(pred, curr);
	                        }
	                        pred = curr;
	                        curr = succ;
	                    }
        			}
        }catch (NovaIllegalAccess e) {continue CmpFail;}
    }



	public <R>R get(K key, NovaR Reader, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
                Node curr = head.next.getReference();
                curr.next.get(marked);
                while (curr != tail && Facade_Slice.Compare(key, Kcm, curr.key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                boolean flag =  curr.key == null ? false : Facade_Slice.Compare(key, Kcm, curr.key) == 0 && !marked[0];
                R obj = null;
                if(flag) 
                	obj = (R) Facade_Slice.Read(Reader, curr.value);
                return obj;
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
    }
    
    public boolean contains(K key, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
                Node curr = head.next.getReference();
                curr.next.get(marked);
                while (curr != tail && Facade_Slice.Compare(key, Kcm, curr.key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                return curr.key == null ? false : Facade_Slice.Compare(key, Kcm, curr.key) == 0 && !marked[0];
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
    }
    
    
    public boolean putIfAbsentOak(K key, V value,  int idx) {
        CmpFail: while(true)
        try{
        	while (true) {
                final Window window = find(key, idx);
                // On Harris paper, pred is named left_node and curr is right_node
                final Node pred = window.pred;
                final Node curr = window.curr;
                if (curr.key!= null && Facade_Slice.Compare(key, Kcm, curr.key) == 0) { 
                    return false;
                } else {
                    
                	Node newNode = new Node(new Facade_slice(), new Facade_slice());
                	
					Facade_Slice.AllocateSlice(newNode.key,Ksr.calculateSize(key), idx);
					Facade_Slice.AllocateSlice(newNode.value,Vsr.calculateSize(value), idx);
                    	
					Facade_Slice.WriteFast(Ksr, key, newNode.key, idx);
					Facade_Slice.WriteFast(Vsr, value, newNode.value, idx);
                    
                    newNode.next.set(curr, false);
                    if (pred.next.compareAndSet(curr, newNode, false, false)) {
                        return true;
                    }
                    else {
                    	Facade_Slice.DeletePrivate(idx, newNode.key);
                    	Facade_Slice.DeletePrivate(idx, newNode.value);
                    }
                }
            }  
        }catch(NovaIllegalAccess e) {continue CmpFail;}
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