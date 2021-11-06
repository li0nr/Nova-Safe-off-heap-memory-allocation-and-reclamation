package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.Stack;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_reuse implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Facade_slice> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static long OAK_MAX_OFF_MEMORY = 256 * GB;
    private static Stack<Facade_slice>[] reused = new  Stack[_Global_Defs.MAX_THREADS*2*_Global_Defs.CACHE_PADDING];

    public SkipList_OffHeap_reuse(long MemCap) {
    	for (int i = _Global_Defs.CACHE_PADDING; i < _Global_Defs.MAX_THREADS * _Global_Defs.CACHE_PADDING* 2; i+=_Global_Defs.CACHE_PADDING) {
    		reused[i] = new Stack<Facade_slice>();
    	}
    	if(MemCap != -1)
    		OAK_MAX_OFF_MEMORY = MemCap;
        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        NovaManager mng = new NovaManager(allocator);
        new Facade_Slice(mng);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		Facade_slice value = skipListMap.get(key);
		if(value == null)
			return null;
		else {
			Integer ret = (Integer)Facade_Slice.Read(Buff.DEFAULT_R, value);
			if(ret == null) //meaning the key was found  but the value is now deleted due to concurrent delete
				return containsKey(key, tidx);
			else return ret;
		}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	Facade_slice offValue;
    	if(!reused[_Global_Defs.CACHE_PADDING*(idx+1)].empty())
    		offValue = reused[_Global_Defs.CACHE_PADDING*(idx+1)].pop();
    	else offValue = new Facade_slice();
    	Facade_Slice.AllocateSlice(offValue, Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	Buff keyb = Buff.CC.Copy(key);
    	Facade_slice valueOff = skipListMap.put(keyb, Facade_Slice.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	if(valueOff != null) {
        	Facade_Slice.Delete(idx, valueOff); 
        	reused[_Global_Defs.CACHE_PADDING*(idx+1)].add(valueOff);
        	return false;
    	}
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) { 
    	Facade_slice offValue = new Facade_slice();
    	Facade_Slice.AllocateSlice(offValue, Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	Buff keyb = Buff.CC.Copy(key);
    	Facade_slice valueOff = skipListMap.put(keyb, Facade_Slice.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	if(valueOff != null)
        	Facade_Slice.Delete(idx, valueOff); 
    	return valueOff== null ? true : false;
    	
    }
    
    @Override
    public  boolean remove(final Buff key, int idx) {
    	Facade_slice val = skipListMap.remove(key);
    	if(val != null) {
    		Facade_Slice.Delete(idx, val); 
    		reused[_Global_Defs.CACHE_PADDING*(idx+1)].push(val);
    		return true;
    	}
    	else return false;
    }


    @Override
    public void clear() {

        //skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);}); not needed since we close the allocator
        skipListMap = new ConcurrentSkipListMap<>();
        allocator.FreeNative();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        NovaManager mng = new NovaManager(allocator);
        new Facade_Slice(mng);
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
