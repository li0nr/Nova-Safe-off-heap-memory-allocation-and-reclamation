package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Long> skipListMap;
    private BlockMemoryAllocator allocator;
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
		return value == null ? null :(Integer)Facade_Nova.Read(Buff.DEFAULT_R, value);
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	long offValue = Facade_Nova.AllocateSlice(value.calculateSerializedSize(), idx);
    	skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    	
    	long offValue = Facade_Nova.AllocateSlice(value.calculateSerializedSize(), idx);
    	skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	return true;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	Long val = skipListMap.remove(key);
    	return val == null ? false : Facade_Nova.DeletePrivate(idx, val);
    }


    @Override
    public void clear() {

        skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);});
        skipListMap = new ConcurrentSkipListMap<>();

        allocator.close();
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
