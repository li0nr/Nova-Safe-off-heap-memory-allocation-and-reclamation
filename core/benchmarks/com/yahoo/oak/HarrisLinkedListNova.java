package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicMarkableReference;

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
public class HarrisLinkedListNova<E> {

    final Node<Facade> head;
    final Node<Facade> tail;
    
    final NovaComparator<E> Cmp;
    final NovaSerializer<E> Srz;
    final NovaManager nm;
    
    final static int MAXTHREADS = 32;
    final ArrayList<Facade> Facades;
    
    static class Node<E> {
        final Facade key;
        final AtomicMarkableReference<Node<Facade>> next;
               
        Node(Facade key) {
            this.key = key;
            this.next = new AtomicMarkableReference<Node<Facade>>(null, false);
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
    
    
    public HarrisLinkedListNova(NovaManager novaManager,NovaComparator<E> cmp,	NovaSerializer<E> srz) {
    	Facades = new ArrayList<>();
		for(int i=0; i<MAXTHREADS;i++)
			Facades.add(new Facade<>());
		Facade x= new Facade<>(novaManager);
		
		nm = novaManager; Cmp = cmp; Srz = srz;
    	
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
		Facade newF = new Facade();
		newF.AllocateSlice(Srz.calculateSize(key),0);
		newF.WriteFull(Srz,key, 0);
        final Node<Facade> newNode = new Node<>(newF);
        while (true) {
            final Window<Facade> window = find(key, tidx);
            // On Harris paper, pred is named left_node and curr is right_node
            final Node<Facade> pred = window.pred;
            final Node<Facade> curr = window.curr;
            if (curr.key == key) { 
                return false;
            } else {
                newNode.next.set(curr, false);
                if (pred.next.compareAndSet(curr, newNode, false, false)) {
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
            final Window<Facade> window = find(key, tidx);
            // On Harris's paper, "pred" is named "left_node" and the "curr"
            // variable is named "right_node".            
            final Node<Facade> pred = window.pred;
            final Node<Facade> curr = window.curr;
            if (curr.key != key) {
                return false;
            } 
            final Node<Facade> succ = curr.next.getReference();
            // In "The Art of Multiprocessor Programming - 1st edition", 
            // the code shown has attemptMark() but we can't use it, 
            // because attemptMark() returns true if the node
            // is already marked, which is not the desired effect, so we 
            // must use compareAndSet() instead.
            if (!curr.next.compareAndSet(succ, succ, false, true)) {
                continue;
            }
            pred.next.compareAndSet(curr, succ, false, false);
            return true;
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
    public Window<Facade> find(E key, int tidx) {
        Node<Facade> pred = null;
        Node<Facade> curr = null; 
        Node<Facade> succ = null;
        boolean[] marked = {false};

        // I think there is a special case for an empty list
        if (head.next.getReference() == tail) {
            return new Window<Facade>(head, tail);
        }
        
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
                    curr = succ;
                    succ = curr.next.get(marked);
                }

                if (curr == tail || Cmp.compareKeyAndSerializedKey(key,curr.key, tidx) <= 0) {
                    return new Window<Facade>(pred, curr);
                }
                pred = curr;
                curr = succ;
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
        Node<Facade> curr = head.next.getReference();
        curr.next.get(marked);
        while (curr != tail && Cmp.compareKeyAndSerializedKey(key,curr.key ,tidx) > 0) {
            curr = curr.next.getReference();
            curr.next.get(marked);
        }
        return Cmp.compareKeyAndSerializedKey(key,curr.key,tidx)==0 && !marked[0];
    }
    
    public boolean computeIfPresent(E key, E newKey, int tidx) {
        boolean[] marked = {false};
        Node<Facade> curr = head.next.getReference();
        curr.next.get(marked);
        while (curr != tail && Cmp.compareKeyAndSerializedKey(key,curr.key,tidx) > 0) {
            curr = curr.next.getReference();
            curr.next.get(marked);
        }
        if( Cmp.compareKeyAndSerializedKey(key,curr.key,tidx)==0 && !marked[0]) {
        	curr.key.WriteFull(Srz, newKey, 0);
        	return true;
        	}
        else return false;
        }
    
	public static void main(String[] args) {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    
	    Buffer x =new Buffer(4);
	    x.set(88);
	    HarrisLinkedListNova<Buffer> List = new HarrisLinkedListNova<>(novaManager, Buffer.DEFAULT_COMPARATOR, Buffer.DEFAULT_SERIALIZER);
		List.add(x,0);
		x.set(120);
		List.add(x,0);
	    Buffer xy =new Buffer(4);
	    Buffer z= new Buffer(128);
	    xy.set(110);
	    List.add(xy,0);
	    List.contains(x,0);
	    List.computeIfPresent(x, z,0);
	    
	    assert List.contains(x,0) == false;
	    assert List.contains(z,0) == true;

	}
	
}