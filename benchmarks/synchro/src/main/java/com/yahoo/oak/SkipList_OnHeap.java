package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OnHeap implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Buff> skipListMap;

    public SkipList_OnHeap() {
        skipListMap = new ConcurrentSkipListMap<>();
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		Buff value = skipListMap.get(key);
		return value == null ? null : (Integer)Buff.GCR.apply(value);
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	Buff keyb = Buff.CC.Copy(key);
    	Buff valueb = Buff.CC.Copy(value);
    	return skipListMap.put(keyb,valueb) == null ? true : false;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    	
    	Buff keyb = Buff.CC.Copy(key);
    	Buff valueb = Buff.CC.Copy(value);
    	return skipListMap.put(keyb,valueb) == null ? true : false;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	Buff val = skipListMap.remove(key);
    	return val == null ? false : true;
    }


    @Override
    public void clear() {

        skipListMap = null;
        skipListMap = new ConcurrentSkipListMap<>();
        System.gc();
    }

    @Override
    public int Size() {
        return skipListMap.size();
    }

    @Override
    public long allocated() {
    	return 0;
    }

    @Override
    public void print() {}

}
