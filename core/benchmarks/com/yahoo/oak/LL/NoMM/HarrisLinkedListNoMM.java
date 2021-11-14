package com.yahoo.oak.LL.NoMM;

import java.util.concurrent.atomic.AtomicMarkableReference;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.Nova.LL_Nova_primitive_noCAS;

public class HarrisLinkedListNoMM <K,V>{

	    final Node head;
	    final Node tail;
	    
	    final NovaC<K> Kcm;
	    final NovaS<K> Ksr;
	    final NovaC<V> Vcm;
	    final NovaS<V> Vsr;
	    final NativeMemoryAllocator allocator;
	    
	    static class Node {
	        final NovaSlice key;
	        final NovaSlice value;
	        final AtomicMarkableReference<Node> next;
	               
	        Node(NovaSlice key, NovaSlice value) {
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


	    public HarrisLinkedListNoMM(NativeMemoryAllocator allocator,NovaC<K> cmp,	NovaS<K> srz,
	    		NovaC<V> Vcmp,	NovaS<V> Vsrz) {
	    	this.allocator = allocator;
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
	    public boolean add(K key,V value, int tidx) {
	        while (true) {
	            final Window window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key != null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) {
	                //Vsr.serialize(value,curr.value.address + curr.value.offset);
	            	UnsafeUtils.putInt(curr.value.address + curr.value.offset+4,
	            			~UnsafeUtils.getInt(curr.value.address + curr.value.offset+4));//4 for capacity
	            	return true;
	            } else {
	    	    	NovaSlice myK = new NovaSlice(0,0,0);
	    	    	NovaSlice myV = new NovaSlice(0,0,0);

	    			allocator.allocate(myK, Ksr.calculateSize(key));
	    			Ksr.serialize(key, myK.address+myK.offset);
	    			allocator.allocate(myV, Vsr.calculateSize(value));
	    			Vsr.serialize(value, myV.address+myV.offset);
	    			
	    			final Node newNode = new Node(myK, myV);
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                    return true;
	                }
                    else {
                    	allocator.free(newNode.key);
                    	allocator.free(newNode.value);
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
	    public boolean remove(K key, int tidx) {
	        while (true) {
	            final Window window = find(key, tidx);
	            // On Harris's paper, "pred" is named "left_node" and the "curr"
	            // variable is named "right_node".            
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key == null || Kcm.compareKeys(curr.key.address + curr.key.offset, key) != 0) {
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
	    public Window find(K key, int tidx) {
	        Node pred = null;
	        Node curr = null; 
	        Node succ = null;
	        boolean[] marked = {false};

	        // I think there is a special case for an empty list
	        if (head.next.getReference() == tail) {
	            return new Window(head, tail);
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
	        		
	                if (curr == tail || Kcm.compareKeys(curr.key.address + curr.key.offset, key) >= 0) {
	                    return new Window(pred, curr);
	                }
	                pred = curr;
	                curr = succ;
	            }
	        }
	    }


	    public <R> R get(K key,NovaR Reader, int tidx) {
	        boolean[] marked = {false};
	        Node curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && Kcm.compareKeys(curr.key.address + curr.key.offset, key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        boolean flag = curr.key == null? false: Kcm.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	        R obj = null;
	        if(flag) {
	        	obj = (R)Reader.apply(curr.value.address+curr.value.offset);
	        }
	        return obj;
	    }
	    
	    public boolean contains(K key, int tidx) {
	        boolean[] marked = {false};
	        Node curr = head.next.getReference();
	        curr.next.get(marked);
	        while (curr != tail && Kcm.compareKeys(curr.key.address + curr.key.offset, key) < 0) {
	            curr = curr.next.getReference();
	            curr.next.get(marked);
	        }
	        return curr.key == null? false: Kcm.compareKeys(curr.key.address + curr.key.offset, key)==0 && !marked[0];
	    }
	    
	    
	    public boolean Fill(K key,V value, int tidx) {
	        while (true) {
	            final Window window = find(key, tidx);
	            // On Harris paper, pred is named left_node and curr is right_node
	            final Node pred = window.pred;
	            final Node curr = window.curr;
	            if (curr.key != null && Kcm.compareKeys(curr.key.address + curr.key.offset, key) == 0) {
	                return false;
	            } else {
	    	    	NovaSlice myK = new NovaSlice(0,0,0);
	    	    	NovaSlice myV = new NovaSlice(0,0,0);

	    			allocator.allocate(myK, Ksr.calculateSize(key));
	    			Ksr.serialize(key, myK.address+myK.offset);
	    			allocator.allocate(myV, Vsr.calculateSize(value));
	    			Vsr.serialize(value, myV.address+myV.offset);
	    			
	    			final Node newNode = new Node(myK, myV);
	                newNode.next.set(curr, false);
	                if (pred.next.compareAndSet(curr, newNode, false, false)) {
	                    return true;
	                }
                    else {
                    	allocator.free(newNode.key);
                    	allocator.free(newNode.value);
                    }	
	            }
	        }       
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
	        	Kcm.Print(curr.key.address+curr.key.offset);
	        	System.out.print("-->");
	        	curr = curr.next.getReference();
	        }
	    }
	    
		public static void main(String[] args) {
		    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
		    
		    Buff x =new Buff(4);
		    x.set(0);
		    HarrisLinkedListNoMM<Buff, Buff>List = new HarrisLinkedListNoMM<>(allocator,
					Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
			List.add(x,x,0);
			List.add(x, x, 0);
			List.get(x, Buff.DEFAULT_R, 0);


		}
}