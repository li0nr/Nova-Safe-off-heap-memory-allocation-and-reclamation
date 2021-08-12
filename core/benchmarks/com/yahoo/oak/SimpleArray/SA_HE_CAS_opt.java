package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.yahoo.oak.HazardEras;

import sun.misc.Unsafe;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.HazardEras.HEslice;
import com.yahoo.oak.SimpleArray.SA_EBR_CAS_opt.FillerThread;

public class SA_HE_CAS_opt {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
    final HazardEras _HE= new HazardEras(allocator);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(HEslice[].class);
			slices_scale = UNSAFE.arrayIndexScale(HEslice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private HEslice[] Slices;

	
	public SA_HE_CAS_opt(NovaS srZ){
		Slices = new HEslice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_HE_CAS_opt(int capacity,NovaS srZ ){
		Slices = new HEslice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = _HE.allocate(srZ.calculateSize(e));
		srZ.serialize(e, Slices[size].address+Slices[size].offset);
		size++;
		return true;
	}
	
	public boolean ParallelFill(int size) {
		ArrayList<Thread> threads = new ArrayList<>();
		int NUM_THREADS = size/1_000_000;;
	    for (int i = 0; i < NUM_THREADS; i++) {
	    	threads.add(new Thread(new FillerThread(i, Slices)));
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
	
	
	public <R> R get(int index, NovaR Reader, int threadIDX) {
		HEslice access = _HE.get_protected(Slices[index],threadIDX);
		if(access == null) {
			_HE.clear(threadIDX);
			return null;
		}
		R obj = (R) Reader.apply(access.address+access.offset);
		_HE.clear(threadIDX);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index]== null) {
			HEslice toEnter = _HE.allocate(srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				_HE.fastFree(toEnter);
				return false;
				}
			}
		HEslice access = _HE.get_protected(Slices[index],threadIDX);
		if(access == null) {
			_HE.clear(threadIDX);
			return false;
			}
		srZ.serialize(obj, access.address+access.offset);
		_HE.clear(threadIDX);
		return true;
	}
	
	public boolean delete(int index, int threadIDX) {
		HEslice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, toDel, null))
			return false;
		_HE.retire(threadIDX, toDel);
		return true;
	}
	
	public int getSize(){
		return size;
	}

	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
	public void close() {
		allocator.close();
	}
	
	public class FillerThread extends Thread {

		int idx;
		HEslice[] array;
		Random localRanom;
		FillerThread(int index, HEslice[] local){
			idx = index;
			array = local;
			localRanom = new Random(idx);
		}
		
		@Override
		public void run() {
			int i = 0;
			int v ;
	        Buff key = new Buff(1024);
   
			while( i < 1_000_000) {		
				v = localRanom.nextInt();
		        key.set(v);		
				array[idx*1_000_000 + i] = _HE.allocate(srZ.calculateSize(key));
				srZ.serialize(key, Slices[idx*1_000_000 + i].address+Slices[idx*1_000_000 + i].offset);
				i++;
				}
			
	        }
	}
}