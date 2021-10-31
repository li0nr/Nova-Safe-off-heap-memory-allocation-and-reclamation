package com.yahoo.oak;

import com.yahoo.oak.HazardEras.HEslice;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_HE implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, HEslice> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;
    private HazardEras mng ;

    public SkipList_OffHeap_HE() {

        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new HazardEras(allocator);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		HEslice value = skipListMap.get(key);
		if(value == null) {
			return null;
		}
		else {
			HEslice obj = mng.get_protected(value, tidx);
			if (obj == null) //we found key but we could not read in time!
				return containsKey(key, tidx);
			Integer ret = (Integer)Buff.DEFAULT_R.apply(obj.address+obj.offset);		        
			mng.clear(tidx);
			return ret;
			}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	HEslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	HEslice valueOff = skipListMap.put(key, offValue);
    	if(valueOff != null)
    		mng.retire(idx, valueOff);
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    
    	HEslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	HEslice valueOff = skipListMap.put(key, offValue);
    	if(valueOff != null)
    		mng.retire(idx, valueOff);
    	return valueOff == null ? true : false;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	HEslice val = skipListMap.remove(key);
    	if(val != null) {
    		mng.retire(idx, val);
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
        mng = new HazardEras(allocator);
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
