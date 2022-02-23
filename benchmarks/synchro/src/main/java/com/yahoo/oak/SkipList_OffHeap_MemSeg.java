package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;
import jdk.incubator.foreign.MemorySegment;



public class SkipList_OffHeap_MemSeg implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, MemorySegment> skipListMap;
    private MemorySegmentAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static long OAK_MAX_OFF_MEMORY = 256 * GB;

    public SkipList_OffHeap_MemSeg(long MemCap) {
    	Module module = com.yahoo.oak.SkipList_OffHeap_MemSeg.class.getModule();

    	String moduleName = module.getName();

    	if(MemCap != -1)
    		OAK_MAX_OFF_MEMORY = MemCap;
        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new MemorySegmentAllocator(OAK_MAX_OFF_MEMORY);
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
    	MemorySegment s = null;
    	assert allocator.allocate(s, Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value,s);
    	skipListMap.put(key,s);
    	MemorySegment valueOff = skipListMap.put(key, s);
    	if(valueOff != null) {
    		allocator.free(s);
    	}
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    
    	MemorySegment s = null;
    	allocator.allocate(s, Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value,s);
    	skipListMap.put(key,s);
    	MemorySegment valueOff = skipListMap.put(key, s);
    	if(valueOff != null)
    		allocator.free(s);
    	return valueOff== null ? true : false;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	MemorySegment val = skipListMap.remove(key);
    	if (val == null )
    		return false ;
    	else {
    		allocator.free(val);
    		return true;
    	}
    }


    @Override
    public void clear() {

        //skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);}); not needed since we close the allocator
        skipListMap = new ConcurrentSkipListMap<>();
        allocator.FreeNative();
        allocator = new MemorySegmentAllocator(OAK_MAX_OFF_MEMORY);
        System.gc();
    }

    @Override
    public int Size() {
        return skipListMap.size();
    }

    @Override
    public long allocated() {
    	return allocator.allocated();
    }

    @Override
    public void print() {}

}
