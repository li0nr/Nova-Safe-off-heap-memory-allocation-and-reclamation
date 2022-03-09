package com.yahoo.oak.NotUsed;

import com.yahoo.oak.HazardEras.HEslice;
import com.yahoo.oak.HazardEras;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_Keys_HE implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<HEslice, KeyValue> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;
    
    private HazardEras mng ;
    final NovaC<Buff> Kcm = Buff.DEFAULT_C;
    private Comparator<Object> comparator;
    
    public SkipList_OffHeap_Keys_HE() {

    	  comparator = (o1, o2) -> {
              if (o1 instanceof Buff) {
            	  if(o2 instanceof Buff) {
            		  return ((Buff)o1).compareTo((Buff)o2);  
            	  }
            	  else {
            		  HEslice obj2 = mng.get_protected((HEslice)o2,(int)Thread.currentThread().getId());
            		  if(obj2 == null)
            			  throw new NovaIllegalAccess();
                      return (-1) * Buff.DEFAULT_C.compareKeys(obj2.address+obj2.offset, (Buff)o1);
                      }
              }
        	  else {
        		  if (o2 instanceof Buff) {
            		  HEslice obj1 = mng.get_protected((HEslice)o1,(int)Thread.currentThread().getId());
            		  if(obj1 == null)
            			  throw new NovaIllegalAccess();
                      return   Buff.DEFAULT_C.compareKeys(obj1.address+obj1.offset, (Buff)o2);
        		  }
        		  else   {
            		  HEslice obj1 = mng.get_protected((HEslice)o1,(int)Thread.currentThread().getId());
            		  HEslice obj2 = mng.get_protected((HEslice)o2,(int)Thread.currentThread().getId());
            		  if(obj2 == null || obj1 == null)
            			  throw new NovaIllegalAccess();
        			  return  Buff.DEFAULT_C.compareKeys(obj1.address+obj1.offset, obj2.address+obj2.offset);
        		  }
        	  }
          };

          
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new HazardEras(allocator);

    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		KeyValue value = skipListMap.get(key);
		if(value == null) {
			mng.clear(tidx);
			return null;
		}
		else {
			HEslice obj1 = mng.get_protected((HEslice)value.value,(int)Thread.currentThread().getId());
			if(obj1 == null)
				throw new NovaIllegalAccess();
			Integer obj = (Integer)Buff.DEFAULT_R.apply(obj1.address+obj1.offset);		        
			mng.clear(tidx);
			return obj;
			}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
		HEslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
		HEslice offKey = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(key));

    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff.DEFAULT_SERIALIZER.serialize(key, offKey.address+offKey.offset);

    	KeyValue  valueOff = skipListMap.put(offKey, new KeyValue(offKey, offValue));
    	if(valueOff != null) {
    		HEslice obj1 = mng.get_protected((HEslice)valueOff.value,(int)Thread.currentThread().getId());
        	if(obj1 != null)
        		mng.retire((int)Thread.currentThread().getId(), obj1);
    	}
    	mng.clear(idx);
    	return true;
    }
    
    @Override
    public  boolean putIfAbsent(final Buff key,final Buff value, int idx) {

    	HEslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	HEslice offKey = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(key));

    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff.DEFAULT_SERIALIZER.serialize(key, offKey.address+offKey.offset);
    	
    	KeyValue  valueOff = skipListMap.put(offKey, new KeyValue(offKey, offValue));

    	if(valueOff != null)
    		mng.retire(idx, valueOff.value);
    	mng.clear(idx);
    	return true;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	KeyValue key_val = skipListMap.remove(key);
    	if ( key_val == null ) {
    		return false ;
    	}else {
    		mng.retire(idx, key_val.key);
    		mng.retire(idx, key_val.value);
    	}
    	return true;
    }


    @Override
    public void clear() {

//        skipListMap.values().forEach(val -> {	Facade_Nova.DeletePrivate(0, val.key);
//        										Facade_Nova.DeletePrivate(0, val.value);});
        //not needed since we close the allocator

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator.FreeNative();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new HazardEras(allocator);
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

    class KeyValue {
    	HEslice key;
    	HEslice value;
    	
    	KeyValue(HEslice Key, HEslice Value){
    		key = Key;
    		value = Value;
    	}
    }
}
