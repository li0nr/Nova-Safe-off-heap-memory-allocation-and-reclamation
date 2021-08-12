package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

import sun.misc.Unsafe;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.NovaSlice;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_Nova_CAS.FillerThread;

public class SA_NoMM2 {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator((long)10 * ((long) Integer.MAX_VALUE) * 8);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(NovaSlice[].class);
			slices_scale = UNSAFE.arrayIndexScale(NovaSlice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private NovaSlice[] Slices;

	
	public SA_NoMM2(NovaS srZ){
		Slices = new NovaSlice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_NoMM2(int capacity,NovaS srZ ){
		Slices = new NovaSlice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = new NovaSlice(0, 0, 0);
		allocator.allocate(Slices[size], srZ.calculateSize(e));
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
	
	public <R> R get(int index, Function Reader, int threadIDX) {
		NovaSlice toRead = Slices[index];
		if(toRead == null) return null;
		R obj = (R) Reader.apply(toRead.address+toRead.offset);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index] == null) {
			NovaSlice toEnter = new NovaSlice(0, 0, 0);
			allocator.allocate(toEnter, srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				allocator.free(toEnter);
				return false;
			}
		}
		NovaSlice toEnter = Slices[index];
		if(toEnter == null) {
			return false;
		}
		srZ.serialize(obj, toEnter.address+toEnter.offset);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		NovaSlice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale,toDel, null))
			return false;
		return true;
	}

	
	public int getSize(){
		return size;
	}

	public void Clear() {
		allocator.close();
	}
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
	public class FillerThread extends Thread {

		int idx;
		NovaSlice[] array;
		Random localRanom;
		FillerThread(int index, NovaSlice[] local){
			idx = index;
			array = local;
			localRanom = new Random(idx);
		}
		
		@Override
		public void run() {
			int v = localRanom.nextInt();
			int i = 0;
	        Buff key = new Buff(1024);
	        key.set(v);
	        
			while( i < 1_000_000) {		
				array[idx*1_000_000 + i] = new NovaSlice(0, 0, 0);
				allocator.allocate(Slices[idx*1_000_000 + i], srZ.calculateSize(key));
				srZ.serialize(key, Slices[idx*1_000_000 + i].address+Slices[idx*1_000_000 + i].offset);
				i++;
				}
			
	        }
	}
	
}