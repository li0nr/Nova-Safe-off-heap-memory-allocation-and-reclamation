package com.yahoo.oak.LL.EBR;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.EBR;
import com.yahoo.oak.Facade_EBR;
import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.LL.EBR.LL_EBR_noCAS.Node;
import com.yahoo.oak.LL.EBR.LL_EBR_noCAS.Window;

public class LL_EBR_noCAS_opt <K,V>{

	    final Node head;
	    final Node tail;
	    
	    final NovaC<K> Kcm;
	    final NovaS<K> Ksr;
	    final NovaC<V> Vcm;
	    final NovaS<V> Vsr;
	    final EBR mng;
	    
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


	    public LL_EBR_noCAS_opt(NativeMemoryAllocator alloc ,NovaC<K> cmp,	NovaS<K> srz,
	    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {
	    	mng = new EBR(alloc);
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
//	        CmpFail: while(true)
//	            try{
	    	while (true) {
	            final Window window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key != null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) {
	                //Vsr.serialize(value, curr.value.address+curr.value.offset);
	            	UnsafeUtils.putInt(curr.value.address + curr.value.offset+4,
	            			~UnsafeUtils.getInt(curr.value.address + curr.value.offset+4));//4 for capacity
	            	mng.clear(tidx);
	                return true;
	            } else {
	            	EBRslice oKey  = mng.allocate( Ksr.calculateSize(key));
	        		Ksr.serialize(key, oKey.address+oKey.offset);
	        		EBRslice oValue  = mng.allocate( Vsr.calculateSize(value));
	        		Vsr.serialize(value, oValue.address+oValue.offset);
	        		
	        		final Node newNode = new Node(oKey, oValue);
	        		
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                    mng.clear(tidx);
	                	return true;
	                }
	                else {
	                	mng.fastFree(newNode.key);
	                	mng.fastFree(newNode.value);
	                }
	            }
	    	}
//	    	}catch(NovaIllegalAccess e) {continue CmpFail;}
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
//	        CmpFail: while(true)
//	            try{
	        while (true) {
	            final Window window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key == null || Kcm.compareKeys(curr.key.address + curr.key.offset, key) != 0) {
	                mng.clear(tidx);
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
		            mng.clear(tidx);
		            mng.retire(curr.key, tidx);
		            mng.retire(curr.value, tidx);
		            return true;	
		            }
	        	}
//	        }catch(NovaIllegalAccess e) {continue CmpFail;}
    	}

	    
	    public Window find(K key, int tidx) {
	        Node pred = null;
	        Node curr = null; 
	        Node succ = null;
	        boolean[] marked = {false};

	        mng.start_op(tidx); // no need to check for every slice wether its deleted or not
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
	                    mng.retire(curr.key, tidx);
	                    mng.retire(curr.value, tidx);

	                    curr = succ;
	                    succ = curr.next.get(marked);
	                }
	                if (curr == tail || Kcm.compareKeys(curr.key.address + curr.key.offset, key) >= 0) {
	                    return new Window(pred, curr);
	                }
	                pred = curr;
	                curr = succ;
	            }
	        }
	    }

	    public <R> R get(K key, NovaR Reader, int tidx) {
	        boolean[] marked = {false};
	        Node curr = head.next.getReference();
	        curr.next.get(marked);
	        mng.start_op(tidx);
	        while (curr != tail && Kcm.compareKeys(curr.key.address + curr.key.offset, key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean ret = curr.key == null? false: Kcm.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	        R obj = null;
	        if(ret)
	        	obj = (R)Reader.apply(curr.value.address+curr.value.offset);
	        mng.end_op(tidx);
	        return obj;
	    }

	    public boolean contains(K key, int tidx) {
	        boolean[] marked = {false};
	        Node curr = head.next.getReference();
	        curr.next.get(marked);
	        mng.start_op(tidx);
	        while (curr != tail && Kcm.compareKeys(curr.key.address + curr.key.offset, key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean ret = curr.key == null? false: Kcm.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	        mng.end_op(tidx);
	        return ret;
	    }
	    
	    public void ForceCleanUp() {
	    	mng.ForceCleanUp();
	    }
	    
	    
	    public boolean putIfAbsentOak(K key, V value, int tidx) {
	    	CmpFail: while(true)
	    		try{
	    			while (true) {
	    				final Window window = find(key, tidx);
	    				// On Harris paper, pred is named left_node and curr is right_node
	    				final Node pred = window.pred;
	    				final Node curr = window.curr;
	    				if (curr.key != null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) {
	    					return false;
	    					} else {
	    		            	EBRslice oKey  = mng.allocate( Ksr.calculateSize(key));
	    		        		Ksr.serialize(key, oKey.address+oKey.offset);
	    		        		EBRslice oValue  = mng.allocate( Vsr.calculateSize(value));
	    		        		Vsr.serialize(value, oValue.address+oValue.offset);
	        		
	    						final Node newNode = new Node(oKey, oValue);
	        		
	    				        newNode.next.set(curr, false);
	    				        if (pred.next.compareAndSet(curr, newNode, false, false)) {
	    				            mng.clear(tidx);
	    				        	return true;
	    				        }
	    				        else {
	    				        	mng.fastFree(newNode.key);
	    				        	mng.fastFree(newNode.value);
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