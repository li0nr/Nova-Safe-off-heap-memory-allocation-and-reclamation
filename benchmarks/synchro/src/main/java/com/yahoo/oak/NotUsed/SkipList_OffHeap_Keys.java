package com.yahoo.oak.NotUsed;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap_Keys implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Long, KeyValue> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static final long OAK_MAX_OFF_MEMORY = 256 * GB;
    
    final NovaC<Buff> Kcm = Buff.DEFAULT_C;
    private Comparator<Object> comparator;
    
    public SkipList_OffHeap_Keys() {

    	  comparator = (o1, o2) -> {
              if (o1 instanceof Buff) {
            	  if(o2 instanceof Buff)
            		  return ((Buff)o1).compareTo((Buff)o2);
            	  else {
                      Long obj2 = (Long) o2;
                      return (-1) * Facade_Nova.Compare((Buff)o1, Kcm, obj2);
            	  }
              }
        	  else {
        		  if (o2 instanceof Buff) {
                      return  Facade_Nova.Compare( (Buff) o2, Kcm, (long )o1);
        		  }
        		  else   return Facade_Nova.Compare(Kcm, (long) o1, ( long )o2);
        	  }
          };

          
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
        NovaManager mng = new NovaManager(allocator);
        new Facade_Nova(mng);
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		for(int i=0 ; i < 1024; i ++) {
			try {
				KeyValue key_val = skipListMap.get(key);
				if(key_val == null)
					return null;
				else {
					Integer ret = (Integer)Facade_Nova.Read(Buff.DEFAULT_R, key_val.value);
					if(ret == null)
						return containsKey(key, tidx);
					else return ret;
				}
			}catch(NovaIllegalAccess e ) {
				continue;
			}
		}
		throw new NovaIllegalAccess();
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
		for(int i = 0 ; i < 1024; i ++) {
			try {
		    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
		    	long offKey = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(key), idx);
		    	Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, key, offKey, idx);
		    	Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx);
		    	
		    	KeyValue key_val = skipListMap.put(offKey, new KeyValue(offKey, offValue) );
		    	
		    	if(key_val != null)
		        	Facade_Nova.Delete(idx, key_val.value); 
		    	return true;
			}catch(NovaIllegalAccess e ) {
				continue;
			}
		}
		throw new NovaIllegalAccess();
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    
    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	long offKey = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(key), idx);
    	Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, key, offKey, idx);
    	Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx);
    	
    	KeyValue key_val = skipListMap.put(offKey, new KeyValue(offKey, offValue) );
    	
    	if(key_val != null)
        	Facade_Nova.Delete(idx, key_val.value); 
    	return true;    	
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
		for(int i=0 ; i < 1024; i ++) {
			try {
		    	KeyValue key_val = skipListMap.remove(key);
		    	if ( key_val == null ) 
		    		return false ;
		    		else {
		    			Facade_Nova.Delete(idx, key_val.key);
		    			Facade_Nova.Delete(idx, key_val.value);
		    		}
		    	return true;
			}catch(NovaIllegalAccess e ) {
				continue;
			}
		}
		throw new NovaIllegalAccess();
    }


    @Override
    public void clear() {

//        skipListMap.values().forEach(val -> {	Facade_Nova.DeletePrivate(0, val.key);
//        										Facade_Nova.DeletePrivate(0, val.value);});
        //not needed since we close the allocator

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator.FreeNative();
        allocator = new NativeMemoryAllocator(OAK_MAX_OFF_MEMORY);
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

    class KeyValue {
    	long key;
    	long value;
    	
    	KeyValue(long Key, long Value){
    		key = Key;
    		value = Value;
    	}
    }
}
