package com.yahoo.oak.LL;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;
import com.yahoo.oak.LL.HarrisLinkedListHE.Node;
import com.yahoo.oak.LL.HarrisLinkedListHE.Window;
import com.yahoo.oak.LL.HarrisLinkedListNova.LLIterator;

public class HarrisLinkedListNoMM <E> implements Iterable<E>{

	    final Node<NovaSlice> head;
	    final Node<NovaSlice> tail;
	    
	    final NovaC<E> Cmp;
	    final NovaS<E> Srz;
	    final NativeMemoryAllocator allocator;
	    final static int MAXTHREADS = 32;
	    
	    static class Node<E> {
	        final NovaSlice key;
	        final AtomicMarkableReference<Node<NovaSlice>> next;
	               
	        Node(NovaSlice key) {
	            this.key = key;
	            this.next = new AtomicMarkableReference<Node<NovaSlice>>(null, false);
	        }
	        
	        public <E> E Read(NovaS<E> Srz) {
	        	return Srz.deserialize(key.address+key.offset);
	        }
	        
	        public Node getNext() {
	        	return this.next.getReference();
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


	    public HarrisLinkedListNoMM(NativeMemoryAllocator allocator,NovaC<E> cmp,	NovaS<E> srz) {
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
	        while (true) {
	            final Window<NovaSlice> window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<NovaSlice> pred = window.pred;
	            final Node<NovaSlice> curr = window.curr;
	            if (curr.key != null && Cmp.compareKeys(curr.key.address + curr.key.offset, key) == 0) {
	                return false;
	            } else {
	    	    	NovaSlice access = new NovaSlice(0,0,0);
	    			allocator.allocate(access, Srz.calculateSize(key));
	    			Srz.serialize(key, access.address+access.offset);
	    			
	    			final Node<NovaSlice> newNode = new Node<>(access);
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
	            final Window<NovaSlice> window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node<NovaSlice> pred = window.pred;
	            final Node<NovaSlice> curr = window.curr;
	            if (curr.key == null || Cmp.compareKeys(curr.key.address + curr.key.offset, key) != 0) {
	                return false;
	            } 
	            final Node<NovaSlice> succ = curr.next.getReference();
	            // In "The Art of Multiprocessor Programming - 1st edition", 
	            // the code shown has attemptMark() but we can't use it, 
	            // because attemptMark() returns true if the node
	            // is already marked, which is not the desired effect, so we 
	            // must use compareAndSet() instead.
	            if (!curr.next.compareAndSet(succ, succ, false, true)) {
	                continue;
	            }
	            if(pred.next.compareAndSet(curr, succ, false, false)) 
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
	    public Window<NovaSlice> find(E key, int tidx) {
	        Node<NovaSlice> pred = null;
	        Node<NovaSlice> curr = null; 
	        Node<NovaSlice> succ = null;
	        boolean[] marked = {false};

	        // I think there is a special case for an empty list
	        if (head.next.getReference() == tail) {
	            return new Window<NovaSlice>(head, tail);
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
	        		
	                if (curr == tail || Cmp.compareKeys(curr.key.address + curr.key.offset, key) >= 0) {
	                    return new Window<NovaSlice>(pred, curr);
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
	        Node<NovaSlice> curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && Cmp.compareKeys(curr.key.address + curr.key.offset, key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        return curr.key == null? false: Cmp.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	    }
	    
	    
	    public Iterator<E> iterator() {
	        return new LLIterator<E>(this);
	    }
	    
	    public void Print() {
	    	Node<NovaSlice> curr = head.next.getReference();
	        while (curr != tail ) {
	        	Cmp.Print(curr.key.address+curr.key.offset);
	        	System.out.print("-->");
	        	curr = curr.next.getReference();
	        }
	    }
	    
	    
	    class LLIterator<E> implements Iterator<E> {
	        Node current;

		   public LLIterator(HarrisLinkedListNoMM<E> list)
		   {
		        current = list.head;
	        }
	        // Checks if the next element exists
	        public boolean hasNext() {
	            return current != null; 	
	        }
	          
	        // moves the cursor/iterator to next element
	        public E next() {
	            E data = (E)current.Read(Srz);
	            current = current.getNext();
	            return data;
	        }
	    }
}