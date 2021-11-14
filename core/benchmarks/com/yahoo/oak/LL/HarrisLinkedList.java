package com.yahoo.oak.LL;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.CopyConstructor;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.Buff.Buff.GCReader;
import com.yahoo.oak.LL.Nova.LL_Nova_primitive_noCAS;

public class HarrisLinkedList <E extends Comparable<? super E>,V extends Comparable<? super V>>{

	    final Node<E,V> head;
	    final Node<E,V> tail;
	    final CopyConstructor<E> KCC;
	    final CopyConstructor<V> VCC;

	    static class Node <E,V> {
	        final E key;
	        V value;
	        final AtomicMarkableReference<Node<E,V>> next;
	               
	        Node(E key, V value) {
	            this.key = key;
	            this.value = value;
	            this.next = new AtomicMarkableReference<Node<E,V>>(null, false);
	        }
	        
	    }
	    
	    // Figure 9.24, page 216
	    static class Window<T,R> {
	        public Node<T,R> pred;
	        public Node<T,R> curr;
	        
	        Window(Node<T,R> myPred, Node<T,R> myCurr) {
	            pred = myPred; 
	            curr = myCurr;
	        }
	    }


	    public HarrisLinkedList(CopyConstructor<E> Kcp,CopyConstructor<V> Vcp) {

	        tail = new Node<E,V>(null,null);
	        head = new Node<E,V>(null,null);
	        head.next.set(tail, false);
	        KCC = Kcp;
	        VCC = Vcp;

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
	    public boolean add(E key,V value, int tidx) {	
	        while (true) {
	            final Window<E,V> window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<E,V> pred = window.pred;
	            final Node<E,V> curr = window.curr;
	            if (curr.key != null && curr.key.compareTo(key) == 0) { 
	                VCC.overWrite(curr.value);
	                return true;
	            } else {
	    	    	E myKey = KCC.Copy(key);
	    	    	V myval= VCC.Copy(value);

	    			final Node<E,V> newNode = new Node(myKey, myval);
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
	            final Window<E,V> window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node<E,V> pred = window.pred;
	            final Node<E,V> curr = window.curr;
	            if (curr.key == null || curr.key.compareTo(key) != 0) {
	                return false;
	            } 
	            final Node<E,V> succ = curr.next.getReference();
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
	    public Window<E,V> find(E key, int tidx) {
	        Node<E,V> pred = null;
	        Node<E,V> curr = null; 
	        Node<E,V> succ = null;
	        boolean[] marked = {false};

	        // I think there is a special case for an empty list
	        if (head.next.getReference() == tail) {
	            return new Window<E,V>(head, tail);
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
	        		
	                if (curr == tail || curr.key.compareTo(key) >= 0) {
	                    return new Window<E,V>(pred, curr);
	                }
	                pred = curr;
	                curr = succ;
	            }
	        }
	    }
	    
	    
	    public <R> R get(E key, GCReader Reader, int tidx) {
	        boolean[] marked = {false};
	        Node<E,V> curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && curr.key.compareTo(key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean flag = curr.key!= null && curr.key.compareTo(key) == 0 && !marked[0];
	        R obj = null;
	        if(flag) {
	        	obj = (R)Reader.apply(curr.value);
	        }
	        return obj;
	    }

	    public boolean contains(E key, int tidx) {
	        boolean[] marked = {false};
	        Node<E,V> curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && curr.key.compareTo(key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean flag = curr.key!= null && curr.key.compareTo(key) == 0 && !marked[0];
	        return flag;
	    }
	    
	    public boolean Fill(E key,V value, int tidx) {	
	        while (true) {
	            final Window<E,V> window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<E,V> pred = window.pred;
	            final Node<E,V> curr = window.curr;
	            if (curr.key != null && curr.key.compareTo(key) == 0) { 
	                return false;
	            } else {
	    	    	E myKey = key;
	    	    	V myval= value;

	    			final Node<E,V> newNode = new Node(myKey, myval);
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                    return true;
	                }
	            }
	        }       
	    }
	    
	    public boolean FastFill(E key,V value, int tidx) {	
	        while (true) {
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node<E,V> pred = head;
	            final Node<E,V> curr = head.next.getReference();
	            
	            final Node<E,V> newNode = new Node(key, value);
	            newNode.next.set(curr, false);
	            if (pred.next.compareAndSet(curr, newNode, false, false)) {
	            	return true;
	            	}
	            return false;
	        }
	    }
	    
	    public int Size() {
	    	int i =0;
	        boolean[] marked = {false};
	        Node<E,V> curr = head.next.getReference();
	        while (curr != tail) {
	        	i ++;
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        return i;
	    }
	    
		public static void main(String[] args) {

		    Buff x =new Buff(4);
		    x.set(0);
			HarrisLinkedList<Buff,Buff> LL = new HarrisLinkedList<Buff,Buff>(Buff.CC,Buff.CC);

			LL.add(x,x,0);
			LL.add(x, x, 0);
			LL.get(x, Buff.GCR, 0);


		}
}