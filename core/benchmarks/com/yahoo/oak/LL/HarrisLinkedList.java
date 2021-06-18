package com.yahoo.oak.LL;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.CopyConstructor;
import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.LL.HarrisLinkedListNova.Node;


public class HarrisLinkedList <E extends Comparable<? super E>>{

	    final Node<E> head;
	    final Node<E> tail;
	    final CopyConstructor<E> CC;

	    static class Node<E> {
	        final E key;
	        final AtomicMarkableReference<Node<E>> next;
	               
	        Node(E key) {
	            this.key = key;
	            this.next = new AtomicMarkableReference<Node<E>>(null, false);
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


	    public HarrisLinkedList(CopyConstructor<E> cp) {

	        tail = new Node<>(null);
	        head = new Node<>(null);
	        head.next.set(tail, false);
	        CC = cp;
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
	    	E myKey = CC.Copy(key);
			final Node<E> newNode = new Node<>(myKey);
	        while (true) {
	            final Window<E> window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<E> pred = window.pred;
	            final Node<E> curr = window.curr;
	            if (curr.key != null && curr.key.compareTo(key) == 0) { 
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
	            final Window<E> window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node<E> pred = window.pred;
	            final Node<E> curr = window.curr;
	            if (curr.key == null || curr.key.compareTo(key) != 0) {
	                return false;
	            } 
	            final Node<E> succ = curr.next.getReference();
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
	    public Window<E> find(E key, int tidx) {
	        Node<E> pred = null;
	        Node<E> curr = null; 
	        Node<E> succ = null;
	        boolean[] marked = {false};

	        // I think there is a special case for an empty list
	        if (head.next.getReference() == tail) {
	            return new Window<E>(head, tail);
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
	        		
	                if (curr == tail || curr.key.compareTo(key) <= 0) {
	                    return new Window<E>(pred, curr);
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
	        Node<E> curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && curr.key.compareTo(key) > 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean flag = curr.key!= null && curr.key.compareTo(key) == 0 && !marked[0];
	        return flag;
	    }
}