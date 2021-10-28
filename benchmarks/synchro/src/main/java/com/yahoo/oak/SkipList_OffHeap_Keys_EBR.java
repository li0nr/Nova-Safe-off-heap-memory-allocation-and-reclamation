package com.yahoo.oak;

import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_Keys_EBR implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<EBRslice, KeyValue> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;
    
    private EBR mng ;
    final NovaC<Buff> Kcm = Buff.DEFAULT_C;
    private Comparator<Object> comparator;
    
    public SkipList_OffHeap_Keys_EBR() {

    	  comparator = (o1, o2) -> {
              if (o1 instanceof Buff) {
            	  if(o2 instanceof Buff)
            		  return ((Buff)o1).compareTo((Buff)o2);
            	  else {
                      EBRslice obj2 = (EBRslice)o2;
                      return (-1) * Buff.DEFAULT_C.compareKeys(obj2.address+obj2.offset, (Buff)o1);
                      }
              }
        	  else {
        		  if (o2 instanceof Buff) {
                      EBRslice obj1 = (EBRslice)o1;
                      return   Buff.DEFAULT_C.compareKeys(obj1.address+obj1.offset, (Buff)o2);
        		  }
        		  else   {
                      EBRslice obj1 = (EBRslice)o1;
                      EBRslice obj2 = (EBRslice)o2;
        			  return  Buff.DEFAULT_C.compareKeys(obj1.address+obj1.offset, obj2.address+obj2.offset);
        		  }
        	  }
          };

          
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new EBR(allocator);

    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		mng.start_op(tidx);
		KeyValue value = skipListMap.get(key);
		if(value == null) {
			mng.end_op(tidx);
			return null;
		}
		else {
			Integer obj = (Integer)Buff.DEFAULT_R.apply(value.value.address+value.value.offset);		        
			mng.end_op(tidx);
			return obj;
			}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
		mng.start_op(idx);

    	EBRslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	EBRslice offKey = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(key));

    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff.DEFAULT_SERIALIZER.serialize(key, offKey.address+offKey.offset);

    	KeyValue  valueOff = skipListMap.put(offKey, new KeyValue(offKey, offValue));
    	if(valueOff != null)
    		mng.retire(valueOff.value, idx);
    	mng.end_op(idx);
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {

    	EBRslice offValue = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(value));
    	EBRslice offKey = mng.allocate(Buff.DEFAULT_SERIALIZER.calculateSize(key));

    	Buff.DEFAULT_SERIALIZER.serialize(value, offValue.address+offValue.offset);
    	Buff.DEFAULT_SERIALIZER.serialize(key, offKey.address+offKey.offset);
    	
    	KeyValue  valueOff = skipListMap.put(offKey, new KeyValue(offKey, offValue));

    	if(valueOff != null)
    		mng.retire(valueOff.value, idx);
    	mng.end_op(idx);
    	return true;
    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
		mng.start_op(idx);

    	KeyValue key_val = skipListMap.remove(key);
    	if ( key_val == null ) {
    		mng.end_op(idx);
    		return false ;
    	}else {
    		mng.retire(key_val.key, idx);
    		mng.retire(key_val.value, idx);
    	}
    	return true;
    }


    @Override
    public void clear() {

//        skipListMap.values().forEach(val -> {	Facade_Nova.DeletePrivate(0, val.key);
//        										Facade_Nova.DeletePrivate(0, val.value);});
        //not needed since we close the allocator

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        mng = new EBR(allocator);
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

    class KeyValue {
    	EBRslice key;
    	EBRslice value;
    	
    	KeyValue(EBRslice Key, EBRslice Value){
    		key = Key;
    		value = Value;
    	}
    }
}
