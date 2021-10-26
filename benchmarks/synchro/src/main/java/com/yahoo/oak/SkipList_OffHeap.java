package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Long> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public SkipList_OffHeap() {

        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        NovaManager mng = new NovaManager(allocator);
        new Facade_Nova(mng);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		Long value = skipListMap.get(key);
		if(value == null)
			return null;
		else {
			Integer ret = (Integer)Facade_Nova.Read(Buff.DEFAULT_R, value);
			if(ret == null)
				return containsKey(key, tidx);
			else return ret;
		}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	Long valueOff = skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	if(valueOff != null)
        	Facade_Nova.Delete(idx, valueOff, null, 0); 
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    
    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	return true;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	Long val = skipListMap.remove(key);
    	return val == null ? false : Facade_Nova.Delete(idx, val, null, 0);
    }


    @Override
    public void clear() {

        skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);});
        skipListMap = new ConcurrentSkipListMap<>();
        allocator.FreeNative();
        allocator = new NativeMemoryAllocator((long) Integer.MAX_VALUE * 16);
        NovaManager mng = new NovaManager(allocator);
        new Facade_Nova(mng);
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
