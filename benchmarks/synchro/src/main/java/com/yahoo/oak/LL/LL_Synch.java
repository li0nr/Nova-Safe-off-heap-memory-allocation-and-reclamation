package com.yahoo.oak.LL;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.LL.synchrobench.contention.abstractions.Compositional;

public class LL_Synch implements Compositional<Buff>{

	HarrisLinkedList<Buff> LL = new HarrisLinkedList<Buff>(Buff.CC);
	
	public LL_Synch(){
		
	}
	public boolean containsKey(final Buff key, int tidx) {
		return LL.contains(key, tidx);
	}
	
    public  boolean put(final Buff key,  int idx) {
    	return LL.add(key, idx);
    }
    
    public  boolean remove(final Buff key, int idx) {
    	return LL.remove(key, idx);
    }
    
    public long allocated() {
    	return 0;
    }
    
    public void clear() {
    	LL = new HarrisLinkedList<Buff>(Buff.CC);
    }
    
    public void print() {
    	//LL.Print();
    }
}