package com.nova;

import com.nova.Buff.Buff;
import com.nova.EBR.EBRslice;
import com.nova.synchrobench.contention.abstractions.CompositionalLL;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_NoMM implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, NovaSlice> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;

    public SkipList_OffHeap_NoMM(long MemCap) { //added the MemCap to be compatible with the reflection call
        skipListMap = new ConcurrentSkipListMap<>();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		NovaSlice value = skipListMap.get(key);
		if(value == null) {
			return null;
		}
		else {
			Integer obj = (Integer)Buff.DEFAULT_R.apply(value.address+value.offset);		        
			return obj;
			}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	NovaSlice offValue = new NovaSlice(0, 0, 0);
		allocator.allocate(offValue, Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff keyb = Buff.CC.Copy(key);
    	NovaSlice valueOff = skipListMap.put(keyb, offValue);
    	if(valueOff != null)
    		return false;
    	return true;
    }
    
    @Override
    public  boolean putIfAbsent(final Buff key,final Buff value, int idx) {    
    	NovaSlice offValue = new NovaSlice(0, 0, 0);
		allocator.allocate(offValue, Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	NovaSlice valueOff = skipListMap.putIfAbsent(key, offValue);
    	if(valueOff != null)
        	allocator.free(offValue);
    	return valueOff == null ? true : false;
    	
    }
    
    @Override
    public  boolean OverWrite(final Buff key,final Buff value, int idx) {
    	NovaSlice valueOff =skipListMap.compute(key, (old,v)->
    	{	
    		if(v== null) return v;
    		UnsafeUtils.putInt(4 +v.offset+v.getAddress(),
    				~UnsafeUtils.getInt( 4 + v.offset+v.getAddress()));//4 for capacity
    			return v;	
    		});
    	if(valueOff == null)
    		return false;
    	else 	
    		return true;
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	NovaSlice val = skipListMap.remove(key);
    	if(val != null) {
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
