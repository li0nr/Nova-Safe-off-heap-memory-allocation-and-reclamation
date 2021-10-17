package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Nova_FenceFree;
import com.yahoo.oak.Facade_Slice;

import sun.misc.Unsafe;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManagerNoTap;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.SimpleArray.SA_Nova_CAS.FillerThread;

public class SA_Nova_FenceFree {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
    final NovaManagerNoTap mng = new NovaManagerNoTap(allocator);
    
    
	
    static final long ref_base_offset;
    static final long ref_scale;
    
    static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			ref_base_offset = UNSAFE.arrayBaseOffset(long[].class);
			ref_scale = UNSAFE.arrayIndexScale(long[].class);
			 } catch (Exception ex) { throw new Error(ex); }
    }
    
    
    private final NovaS srZ;
	private int size=0;
	private long[] refrences;

	
	public SA_Nova_FenceFree(NovaS srZ){
		new Facade_Nova_FenceFree(mng);
		refrences = new long[DEFAULT_CAPACITY];
		for(int i=0; i <DEFAULT_CAPACITY ; i++)
			refrences[i] = 1;
		this.srZ = srZ;
	}
	
	public SA_Nova_FenceFree(int capacity,NovaS srZ ){
		new Facade_Nova_FenceFree(mng);
		refrences = new long[capacity];
		for(int i=0; i <capacity ; i++)
			refrences[i] = 1;
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == refrences.length) {
			EnsureCap();//might be problematic 
		}
		refrences[size]= Facade_Nova_FenceFree.WriteFast(srZ, e, 
				Facade_Nova_FenceFree.AllocateReusedSlice(refrences,ref_base_offset+size*ref_scale,
						refrences[size],srZ.calculateSize(e),threadIDX), threadIDX);
		size++;
		return true;
	}
	
	public boolean ParallelFill(int size) {
		ArrayList<Thread> threads = new ArrayList<>();
		int NUM_THREADS = size/1_000_000;;
	    for (int i = 0; i < NUM_THREADS; i++) {
	    	threads.add(new Thread(new FillerThread(i, refrences)));
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
		return  Facade_Nova_FenceFree.Read(Reader, refrences[index]);
		}
	
	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(refrences[index] %2 == 1)
			Facade_Nova_FenceFree.AllocateReusedSlice(refrences,ref_base_offset+index*ref_scale,
					refrences[index],srZ.calculateSize(obj),threadIDX);
		return Facade_Nova_FenceFree.WriteFull(srZ, obj, refrences[index], threadIDX) != -1 ? true : false;
	}
	

	public boolean delete(int index, int threadIDX) {
		return Facade_Nova_FenceFree.DeleteReusedSlice(threadIDX, refrences[index],
				refrences,ref_base_offset+index*ref_scale);
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = refrences.length *2;
		refrences = Arrays.copyOf(refrences, newSize);
		for(int i = refrences.length/2 ; i <newSize ; i++)
			refrences[i] = 1;
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
	public class FillerThread extends Thread {

		int idx;
		long[] array;
		Random localRanom;
		FillerThread(int index, long[] local){
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
				array[idx*1_000_000 + i] = Facade_Nova.WriteFast(srZ, key, Facade_Nova.AllocateSlice(key, 1, srZ.calculateSize(key) , idx), idx);
				i++;
				}
			
	        }
	}
	
}