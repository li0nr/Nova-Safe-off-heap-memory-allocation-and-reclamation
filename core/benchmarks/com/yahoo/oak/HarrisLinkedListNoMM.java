package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.HarrisLinkedListHE.Node;
import com.yahoo.oak.HarrisLinkedListHE.Window;

public class HarrisLinkedListNoMM <E>{

	    final Node<NovaSlice> head;
	    final Node<NovaSlice> tail;
	    
	    final NovaC<E> Cmp;
	    final NovaSerializer<E> Srz;
	    final NativeMemoryAllocator allocator;
	    final static int MAXTHREADS = 32;
	    
	    static class Node<E> {
	        final NovaSlice key;
	        final AtomicMarkableReference<Node<NovaSlice>> next;
	               
	        Node(NovaSlice key) {
	            this.key = key;
	            this.next = new AtomicMarkableReference<Node<NovaSlice>>(null, false);
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


	    public HarrisLinkedListNoMM(NativeMemoryAllocator allocator,NovaC<E> cmp,	NovaSerializer<E> srz) {
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
	    	NovaSlice access = new NovaSlice(0,0,0);
			allocator.allocate(access, Srz.calculateSize(key));
			Srz.serialize(key, access.address+access.offset);
			
			final Node<NovaSlice> newNode = new Node<>(access);
	        while (true) {
	            final Window<NovaSlice> window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<NovaSlice> pred = window.pred;
	            final Node<NovaSlice> curr = window.curr;
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
	            final Window<NovaSlice> window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node<NovaSlice> pred = window.pred;
	            final Node<NovaSlice> curr = window.curr;
	            if (curr.key != key) {
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
	        		
	                if (curr == tail || Cmp.compareKeys(curr.key.address + curr.key.offset, key) <= 0) {
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
	        boolean flag = Cmp.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	        return flag;
	    }
	    
	    
		public static void main(String[] args) {
		    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
		    final NovaManager novaManager = new NovaManager(allocator);
		    
		    Buff x =new Buff(4);
		    x.set(88);
		    HarrisLinkedListNoMM<Buff> List = new HarrisLinkedListNoMM<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
			List.add(x,0);
			x.set(120);
			List.add(x,0);
		    Buff xy =new Buff(4);
		    Buff z= new Buff(128);
		    xy.set(110);
		    List.add(xy,0);
		    List.contains(x,0);
		    
		    assert List.contains(x,0) == false; //removed putif apsent may effect this result
		    assert List.contains(z,0) == true;

		}
}