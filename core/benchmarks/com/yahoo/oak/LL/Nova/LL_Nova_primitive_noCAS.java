package com.yahoo.oak.LL.Nova;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.Buff.Buff;
import sun.misc.Unsafe;

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
public class LL_Nova_primitive_noCAS<K,V> {

    final Node head;
    final Node tail;
    
    final NovaC<K> Kcm;
    final NovaS<K> Ksr;
    final NovaC<V> Vcm;
    final NovaS<V> Vsr;
    final NovaManager nm;
    
    final static int MAXTHREADS = 32;
    final static int Illegal_nu = 1;
    final static long key_offset;
    final static long value_offset;
    
	static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			key_offset = UNSAFE.objectFieldOffset
				    (Node.class.getDeclaredField("key"));
			value_offset = UNSAFE.objectFieldOffset
				    (Node.class.getDeclaredField("value"));
			 } catch (Exception ex) { throw new Error(ex); }
	}
    
    static class Node {
        final long key;
        final long value;
        final AtomicMarkableReference<Node> next;
               
        Node(long key, long value) {
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
    
    
    public LL_Nova_primitive_noCAS(NovaManager novaManager,NovaC<K> cmp,	NovaS<K> srz,
    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {	
		nm = novaManager; Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
		new Facade_Nova(nm);
	      
        tail = new Node(Illegal_nu,Illegal_nu);
        head = new Node(Illegal_nu,Illegal_nu);
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
                if (curr.key!= Illegal_nu && Facade_Nova.Compare(key, Kcm, curr.key) == 0) {
                	Facade_Nova.WriteFull(Vsr, value, curr.value, idx);
                    return true;
                } else {

                    long OffKRef = Facade_Nova.WriteFast(Ksr, key, Facade_Nova.AllocateSlice(null, key_offset,
                    		Ksr.calculateSize(key), idx),idx);
                    long OffVRef = Facade_Nova.WriteFast(Vsr, value, Facade_Nova.AllocateSlice(null, value_offset,
                    		Vsr.calculateSize(value), idx),idx);
                    
                    Node newNode = new Node(OffKRef,OffVRef);

                    newNode.next.set(curr, false);
                    if (pred.next.compareAndSet(curr, newNode, false, false)) {
                        return true;
                    }
                    else {
                    	Facade_Nova.DeletePrivate(idx, newNode.key);
                    	Facade_Nova.DeletePrivate(idx, newNode.value);
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
    	try {
            while (true) {
                final Window window = find(key, tidx);
                // On Harris's paper, "pred" is named "left_node" and the "curr"
                // variable is named "right_node".            
                final Node pred = window.pred;
                final Node curr = window.curr;
                if (curr.key == Illegal_nu || Facade_Nova.Compare(key, Kcm, curr.key) != 0) {
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
                    Facade_Nova.Delete(tidx, curr.value, curr, value_offset);
                    Facade_Nova.Delete(tidx, curr.key, curr, key_offset);
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
	                            Facade_Nova.Delete(tidx, curr.key, curr, key_offset);
	                            Facade_Nova.Delete(tidx, curr.value, curr, value_offset);
	                            curr = succ;
	                            succ = curr.next.get(marked);
	                        }
	
	                        if (curr == tail || Facade_Nova.Compare(key, Kcm, curr.key) >= 0) { //we compare the offheap vs the key thus looking for >
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
                while (curr != tail && Facade_Nova.Compare(key, Kcm, curr.key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                return curr.key == Illegal_nu ? false : Facade_Nova.Compare(key, Kcm, curr.key) == 0 && !marked[0];
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
    }
    
    public <R>R get(K key,  NovaR Reader, int tidx) {
        boolean[] marked = {false};
        CmpFail: while(true)
        	try {
                Node curr = head.next.getReference();
                curr.next.get(marked);
                while (curr != tail && Facade_Nova.Compare(key, Kcm, curr.key) < 0) {
                    curr = curr.next.getReference();
                    curr.next.get(marked);
                }
                boolean flag =  curr.key == Illegal_nu ? 
                		false : Facade_Nova.Compare(key, Kcm, curr.key) == 0 && !marked[0];
                R obj = null;
                if(flag) 
                	obj = (R) Facade_Nova.Read(Reader, curr.value);
                return obj;
        	}catch (NovaIllegalAccess e) {continue CmpFail;}
    }
    
    
    public boolean Fill(K key, V value,  int idx) {
        CmpFail: while(true)
        try{
        	while (true) {
                final Window window = find(key, idx);
                // On Harris paper, pred is named left_node and curr is right_node
                final Node pred = window.pred;
                final Node curr = window.curr;
                if (curr.key!= Illegal_nu && Facade_Nova.Compare(key, Kcm, curr.key) == 0) { 
                    return false;
                } else {

                    long OffKRef = Facade_Nova.WriteFast(Ksr, key, Facade_Nova.AllocateSlice(null, key_offset,
                    		Ksr.calculateSize(key), idx),idx);
                    long OffVRef = Facade_Nova.WriteFast(Vsr, value, Facade_Nova.AllocateSlice(null, value_offset,
                    		Vsr.calculateSize(value), idx),idx);
                    
                    Node newNode = new Node(OffKRef,OffVRef);

                    newNode.next.set(curr, false);
                    if (pred.next.compareAndSet(curr, newNode, false, false)) {
                        return true;
                    }
                    else {
                    	Facade_Nova.DeletePrivate(idx, newNode.key);
                    	Facade_Nova.DeletePrivate(idx, newNode.value);
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
    
    public void Print() {
        Node curr = head.next.getReference();
        while (curr != tail ) {
 	       Facade_Nova.Print(Kcm, curr.key);
 	       System.out.print("-->");
           curr = curr.next.getReference();

        }
    }

    
	public static void main(String[] args) {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    
	    Buff x =new Buff(4);
	    x.set(88);
	    LL_Nova_primitive_CAS<Buff, Buff>List = new LL_Nova_primitive_CAS<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		 ,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
		List.add(x,x,0);
		List.remove(x, 0);
		List.add(x,x,0);
		x.set(120);
		List.add(x,x,0);


	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
	    
	    
	    List.Print();
	    assert List.contains(x,0) == false;
	    assert List.contains(z,0) == true;

	}
	
}