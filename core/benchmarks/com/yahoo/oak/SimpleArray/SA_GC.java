package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.oak.CopyConstructor;
import sun.misc.Unsafe;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.Buff.Buff.GCReader;
import com.yahoo.oak.benchmarks.BenchmarkConcurrent.ReaderThread;
import com.yahoo.oak.benchmarks.BenchmarkConcurrent.ReaderThreadSerial;

public class SA_GC {
	

	private static final int DEFAULT_CAPACITY=10;  
	
    static final long slices_base_offset;
    static final long slices_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			slices_base_offset = UNSAFE.arrayBaseOffset(Buff[].class);
			slices_scale = UNSAFE.arrayIndexScale(Buff[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final CopyConstructor<Buff> CC;
	private int size=0;
	private Buff[] Slices;

	
	public SA_GC(CopyConstructor CC){
		Slices = new Buff[DEFAULT_CAPACITY];
		this.CC = CC;
	}
	
	public SA_GC(int capacity,CopyConstructor CC ){
		Slices = new Buff[capacity];
		this.CC = CC;
	}

	public boolean fill(Buff e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = e;
		size++;
		return true;
	}
	
	public boolean Parallelfill(int size) {
		ArrayList<Thread> threads = new ArrayList<>();
		int NUM_THREADS = size/1_000_000;
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
	
	
	public <R> R get(int index, GCReader Reader, int threadIDX) {
		Buff toRead = Slices[index];
		if(toRead == null)
			return null;
		return (R)Reader.apply(Slices[index]);
	}
	
	public boolean set(int index, Buff obj, int threadIDX)  {
		if(Slices[index] == null) {
			Buff toAdd = CC.Copy(obj); 
			if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices,
					slices_base_offset+index*slices_scale, null, toAdd))
				return false;
			return true;
		}
		Slices[index] = CC.Copy(obj);
		return true;
	}
	

	public boolean delete(int index, int threadIDX) {
		Buff toDel = Slices[index];
		if(toDel == null) 
			return false;
		if(!UnsafeUtils.unsafe.compareAndSwapObject(Slices,
				slices_base_offset+index*slices_scale, toDel, null))
			return false;
		else return true;
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
	
	public class FillerThread extends Thread {

		int idx;
		Buff[] array;
		Random localRanom;
		FillerThread(int index, Buff[] local){
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
				array[idx*1_000_000 + i] = key;
				i++;
				}
			
	        }
	}
}