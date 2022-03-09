package com.yahoo.oak;

import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_EBR implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, EBRslice> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static long OAK_MAX_OFF_MEMORY = 256 * GB;
    private EBR mng ;

    public SkipList_OffHeap_EBR(long MemCap) {
    	if(MemCap != -1)
    		OAK_MAX_OFF_MEMORY = MemCap;
        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new EBR(allocator);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		mng.start_op(tidx);
		EBRslice value = skipListMap.get(key);
		if(value == null) {
			mng.end_op(tidx);
			return null;
		}
		else {
			Integer obj = (Integer)Buff.DEFAULT_R.apply(value.address+value.offset);		        
			mng.end_op(tidx);
			return obj;
			}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	EBRslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff keyb = Buff.CC.Copy(key);
    	EBRslice valueOff = skipListMap.put(keyb, offValue);
    	if(valueOff != null)
    		mng.retire(valueOff, idx);
    	return true;
    }
    
    @Override
    public  boolean OverWrite(final Buff key,final Buff value, int idx) {
//    	EBRslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
//    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
//    	Buff keyb = Buff.CC.Copy(key);
		mng.start_op(idx);
    	EBRslice valueOff =skipListMap.compute(key,(k,v)->
    	{	
    		if(v == null) return v;
    		UnsafeUtils.putInt(4 +v.offset+v.getAddress(),
    				~UnsafeUtils.getInt( 4 + v.offset+v.getAddress()));//4 for capacity
    		mng.end_op(idx);
    			return v;	
    		});
    	if(valueOff == null)
    		return false;
    	else 	
    		return true;
    }
    
    @Override
    public  boolean putIfAbsent(final Buff key,final Buff value, int idx) {    
    	EBRslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	EBRslice valueOff = skipListMap.put(key, offValue);
    	if(valueOff != null)
    		mng.fastFree(valueOff);
    	return valueOff == null ? true : false;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	EBRslice val = skipListMap.remove(key);
    	if(val != null) {
    		mng.retire(val, idx);
    		return true;
    	}
    	return false;
    }


    @Override
    public void clear() {

        //skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);}); not needed since we close the allocator
        skipListMap = new ConcurrentSkipListMap<>();
        allocator.FreeNative();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new EBR(allocator);
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }

    @Override
    public long allocated() {
    	return allocator.allocated();
    }

    @Override
    public void print() {}

}
