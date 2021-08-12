package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

import com.yahoo.oak.EBR;
import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.SimpleArray.SA_Nova_Primitive_CAS.FillerThread;

import sun.misc.Unsafe;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;

public class SA_EBR_CAS_opt {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
    final EBR _EBR = new EBR(allocator);
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(EBRslice[].class);
			slices_scale = UNSAFE.arrayIndexScale(EBRslice[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private EBRslice[] Slices;

	
	public SA_EBR_CAS_opt(NovaS srZ){
		Slices = new EBRslice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_EBR_CAS_opt(int capacity,NovaS srZ ){
		Slices = new EBRslice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = _EBR.allocate(srZ.calculateSize(e));
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
		_EBR.start_op(threadIDX);
		EBRslice toRead = Slices[index];
		if(toRead == null) return null;
		R obj = (R) Reader.apply(toRead.address+toRead.offset);
		_EBR.end_op(threadIDX);
		return obj;
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index] == null) {
			EBRslice toEnter = _EBR.allocate(srZ.calculateSize(obj));
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale, null, toEnter)) {
				_EBR.fastFree(toEnter);
				return false;
			}
		}
		_EBR.start_op(threadIDX);
		EBRslice toEnter = Slices[index];
		if(toEnter == null) {
			_EBR.end_op(threadIDX);
			return false;
		}
		srZ.serialize(obj, toEnter.address+toEnter.offset);
		_EBR.end_op(threadIDX);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		EBRslice toDel = Slices[index];
		if(toDel == null)
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices, slices_base_offset+index*slices_scale,toDel, null))
			return false;
		_EBR.retire(toDel, threadIDX);
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
		EBRslice[] array;
		Random localRanom;
		FillerThread(int index, EBRslice[] local){
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
				array[idx*1_000_000 + i] = _EBR.allocate(srZ.calculateSize(key));
				srZ.serialize(key, Slices[idx*1_000_000 + i].address+Slices[idx*1_000_000 + i].offset);
				i++;
				}
			
	        }
	}
}