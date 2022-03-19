package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;
import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;




public class SkipList_OffHeap_MemSeg implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, MemorySegment> skipListMap;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static long OAK_MAX_OFF_MEMORY = 256 * GB;

    public SkipList_OffHeap_MemSeg(long MemCap) {

    	if(MemCap != -1)
    		OAK_MAX_OFF_MEMORY = MemCap;
        skipListMap = new ConcurrentSkipListMap<>();
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		MemorySegment value = skipListMap.get(key);
		if(value == null)
			return null;
		else {
			try {
				Integer ret = (Integer) Buff.MSR.apply(value);
				return ret;
			} catch(IllegalStateException e) {
				return containsKey(key, tidx);
			}
		}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	MemorySegment s = MemorySegment.allocateNative(
    			Buff.DEFAULT_SERIALIZER.calculateSize(value), ResourceScope.newSharedScope());
    	Buff.DEFAULT_SERIALIZER.serialize(value,s);
    	skipListMap.put(key,s);
    	MemorySegment valueOff = skipListMap.put(key, s);
    	if(valueOff != null) {
        	s.scope().close();
    	}
    	return true;
    }
    
    @Override
    public  boolean OverWrite(final Buff key,final Buff value, int idx) {
    	
    	MemorySegment valueOff = skipListMap.compute(key,(k,v)->
    		{	
    			if(v == null) return v;
    			ResourceScope.Handle segmentHandle = v.scope().acquire();
    			try {
    				MemoryAccess.setIntAtOffset(v, 4, ~MemoryAccess.getIntAtOffset(v,4));
    			} finally {
    				   v.scope().release(segmentHandle);
    			}
    			return v;
    		});
    	if(valueOff == null)
    		return false;
    	else 	
    		return true;
    }

		
    @Override
    public  boolean putIfAbsent(final Buff key,final Buff value, int idx) {    
    	MemorySegment s = MemorySegment.allocateNative(
    			Buff.DEFAULT_SERIALIZER.calculateSize(value), ResourceScope.newSharedScope());
    	Buff.DEFAULT_SERIALIZER.serialize(value,s);
    	MemorySegment valueOff = skipListMap.putIfAbsent(key, s);
    	if(valueOff != null)
        	s.scope().close();
    	return valueOff == null ? true : false;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	MemorySegment val = skipListMap.remove(key);
    	if (val == null )
    		return false ;
    	else {
        	val.scope().close();
    		return true;
    	}
    }


    @Override
    public void clear() {

        //skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);}); not needed since we close the allocator
        skipListMap = new ConcurrentSkipListMap<>();
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }

    @Override
    public long allocated() {
    	return -1;
    }

    @Override
    public void print() {}

}
