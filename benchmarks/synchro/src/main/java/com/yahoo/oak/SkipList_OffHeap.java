package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SkipList_OnHeap.FillerThread;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OffHeap implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Long> skipListMap;
    private NativeMemoryAllocator allocator;
    private static final long KB = 1024L;
    private static final long GB = KB * KB * KB;
    private static long OAK_MAX_OFF_MEMORY = 256 * GB;

    public SkipList_OffHeap(long MemCap) {
    	if(MemCap != -1)
    		OAK_MAX_OFF_MEMORY = MemCap;
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
			if(ret == null) //meaning the key was found  but the value is now deleted due to concurrent delete
				return containsKey(key, tidx);
			else return ret;
		}
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	Long valueOff = skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	if(valueOff != null) {
        	Facade_Nova.Delete(idx, valueOff); 
        	return false;
    	}
    	return true;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    
    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(value), idx);
    	Long valueOff = skipListMap.put(key, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, value, offValue, idx));
    	if(valueOff != null)
        	Facade_Nova.Delete(idx, valueOff); 
    	return valueOff== null ? true : false;
    	
    }
    
    public  boolean FillParallel(int sizeInMillions, int keysize, int valsize, int range) {    	
    	ArrayList<Thread> threads = new ArrayList<>();
    	int NUM_THREADS = sizeInMillions/1_000_000;;
	    for (int i = 0; i < NUM_THREADS; i++) {
	    	threads.add(new Thread(new FillerThread(i, skipListMap, keysize, valsize, range)));
	    	threads.get(i).start();
	    	}	
	    for (Thread thread : threads) {
	        try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
		return true;
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	Long val = skipListMap.remove(key);
    	return val == null ? false : Facade_Nova.Delete(idx, val);
    }


    @Override
    public void clear() {

        //skipListMap.values().forEach(val -> {Facade_Nova.DeletePrivate(0, val);}); not needed since we close the allocator
        skipListMap = new ConcurrentSkipListMap<>();
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

	public class FillerThread extends Thread {

		int idx;
		Random localRanom;
		Buff keybuf;
		Buff valbuf;
		int range;
		ConcurrentSkipListMap map;
		
		FillerThread(int index, ConcurrentSkipListMap local, int keysize, int valsize, int range){
			idx = index;
			map = local;
			this.range = range;
			keybuf = new Buff(keysize);
			valbuf = new Buff(valsize);
			localRanom = new Random(idx);
		}
		
		@Override
		public void run() {
			int i = 0;
			int v ;
			while( i < 1_000_000) {		
				v = localRanom.nextInt(this.range);
				keybuf.set(v);
				valbuf.set(v);
		    	Buff keyb = Buff.CC.Copy(keybuf);
		    	long offValue = Facade_Nova.AllocateSlice(Buff.DEFAULT_SERIALIZER.calculateSize(valbuf), idx);
		    	Long valueOff = skipListMap.put(keyb, Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, valbuf, offValue, idx));
		    	if(valueOff != null) {
		        	Facade_Nova.Delete(idx, valueOff); 
					i--;
					}
		    	i++;
		    	}
			}
		}
}
