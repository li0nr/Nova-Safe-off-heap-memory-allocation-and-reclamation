package com.yahoo.oak.LL.EBR;

import java.util.concurrent.atomic.AtomicMarkableReference;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.Facade_EBR;
import com.yahoo.oak.EBR.EBRslice;


public class LL_EBR_noCAS <K,V>{

	    final Node head;
	    final Node tail;
	    
	    final NovaC<K> Kcm;
	    final NovaS<K> Ksr;
	    final NovaC<V> Vcm;
	    final NovaS<V> Vsr;
	    
	    static class Node {
	        final EBRslice key;
	        final EBRslice value;
	        final AtomicMarkableReference<Node> next;
	               
	        Node(EBRslice key, EBRslice value) {
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


	    public LL_EBR_noCAS(NativeMemoryAllocator alloc ,NovaC<K> cmp,	NovaS<K> srz,
	    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {
	    	new Facade_EBR(alloc);
	    	Kcm = cmp; Ksr = srz; Vcm = Vcmp; Vsr = Vsrz;
	    	
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
	    public boolean add(K key, V value, int tidx) {
	        CmpFail: while(true)
	        	try{
	    	while (true) {
	            final Window window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key != null && Facade_EBR.Compare(key, Kcm, curr.key, tidx) == 0) {
	            	Facade_EBR.Write(Vsr, value, curr.value, tidx);
	                return true;
	            } else {
	            	EBRslice oKey  = Facade_EBR.allocate(Ksr.calculateSize(key));
	        		Ksr.serialize(key, oKey.address+oKey.offset);
	        		EBRslice oValue  = Facade_EBR.allocate( Vsr.calculateSize(value));
	        		Vsr.serialize(value, oValue.address+oValue.offset);
	        		
	        		final Node newNode = new Node(oKey, oValue);
	        		
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                	return true;
	                }
	                else {
	                	Facade_EBR.fastFree(newNode.key);
	                	Facade_EBR.fastFree(newNode.value);
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
	            if (curr.key == null || Facade_EBR.Compare(key, Kcm, curr.key, tidx) != 0) {
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
	            	Facade_EBR.Delete(tidx, curr.key);
	            	Facade_EBR.Delete(tidx, curr.value);
		            return true;	
		            }
	        	}
	        }catch(NovaIllegalAccess e) {continue CmpFail;}
    	}

	    
	    public Window find(K key, int tidx) {
	        Node pred = null;
	        Node curr = null; 
	        Node succ = null;
	        boolean[] marked = {false};
	        // I think there is a special case for an empty list
	        if (head.next.getReference() == tail) {
	            return new Window (head, tail);
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
	                    
	                    Facade_EBR.Delete(tidx, curr.key);
	                    Facade_EBR.Delete(tidx, curr.value);
	                    curr = succ;
	                    succ = curr.next.get(marked);
	                }
	        		
	                if (curr == tail || Facade_EBR.Compare(key, Kcm, curr.key, tidx) >= 0) {
	                    return new Window(pred, curr);
	                }
	                pred = curr;
	                curr = succ;
	            }
	        }
	    }
	    
	    public <R> R get(K key, NovaR Reader, int tidx) {
	        boolean[] marked = {false};
	    	CmpFail: while(true)
	    	try {
		        Node curr = head.next.getReference();
		        curr.next.get(marked);
		        while (curr != tail && Facade_EBR.Compare(key, Kcm, curr.key, tidx) < 0) {
		            curr = curr.next.getReference();
		            curr.next.get(marked);
		        }
		        boolean ret = curr.key == null? false: Facade_EBR.Compare(key, Kcm, curr.key, tidx)==0 && !marked[0];
		        R obj = null;
		        if(ret)
		        	obj = (R) Facade_EBR.Read(Reader, curr.key, tidx);
		        return obj;
	    	}catch(NovaIllegalAccess e) {continue CmpFail;}

	    }

	    public boolean contains(K key, int tidx) {
	        boolean[] marked = {false};
	        Node curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && Facade_EBR.Compare(key, Kcm, curr.key, tidx) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean ret = curr.key == null? false: Facade_EBR.Compare(key, Kcm, curr.key, tidx)==0 && !marked[0];
	        return ret;
	    }
	    
	    
	    public boolean Fill(K key, V value, int tidx) {
	        CmpFail: while(true)
	            try{
	    	while (true) {
	            final Window window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key != null && Facade_EBR.Compare(key, Kcm, curr.key, tidx) == 0) {
	                return false;
	            } else {
	            	EBRslice oKey  = Facade_EBR.allocate(Ksr.calculateSize(key));
	        		Ksr.serialize(key, oKey.address+oKey.offset);
	        		EBRslice oValue  = Facade_EBR.allocate( Vsr.calculateSize(value));
	        		Vsr.serialize(value, oValue.address+oValue.offset);
	        		
	        		final Node newNode = new Node(oKey, oValue);
	        		
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                	return true;
	                }
	                else {
	                	Facade_EBR.fastFree(newNode.key);
	                	Facade_EBR.fastFree(newNode.value);
	                }
	            }
	    	}
	    	}catch(NovaIllegalAccess e) {continue CmpFail;}
	    }
	    
	    public void ForceCleanUp() {
	    	Facade_EBR.ForceCleanUp();
	    }
}