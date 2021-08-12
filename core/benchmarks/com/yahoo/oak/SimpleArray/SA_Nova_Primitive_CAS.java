package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.oak.Facade_Nova;
import sun.misc.Unsafe;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.ParamBench;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.EBR.EBRslice;
import com.yahoo.oak.SimpleArray.SA_GC.FillerThread;

public class SA_Nova_Primitive_CAS {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
    final NovaManager mng = new NovaManager(allocator);
    
    
	
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

	
	public SA_Nova_Primitive_CAS(NovaS srZ){
		new Facade_Nova(mng);
		refrences = new long[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_Nova_Primitive_CAS(int capacity,NovaS srZ ){
		new Facade_Nova(mng);
		refrences = new long[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == refrences.length) {
			EnsureCap();//might be problematic 
		}
//		Facade_Nova.AllocateSlice(e, refrences_offset + size*Long.BYTES, srZ.calculateSize(e) , threadIDX);
		//Facade_Nova.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX);
		refrences[size]= Facade_Nova.WriteFast(srZ, e, Facade_Nova.AllocateSlice(e, 1, srZ.calculateSize(e) , threadIDX), threadIDX);
		size++;
		return true;
	}
	
	public boolean Parallelfill(int size) {
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
		try {
			return (R)Facade_Nova.Read(Reader, refrences[index]);
		}catch(NovaIllegalAccess e) {
			return null;
		}
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(refrences[index] %2 == 1)
			Facade_Nova.AllocateReusedSlice(refrences,ref_base_offset+index*ref_scale,
					refrences[index],srZ.calculateSize(obj),threadIDX);
		return Facade_Nova.WriteFull(srZ, obj, refrences[index], threadIDX) != -1 ? true : false; 
	}
	
	public <T> T get(NovaR<T> Reader, int index, int threadIDX)  {
		try {
			return  (T)Facade_Nova.Read(Reader, refrences[index]);
		}catch(NovaIllegalAccess e) {
			return null;
		}
	}
	

	public boolean delete(int index, int threadIDX) {
		return Facade_Nova.DeleteReusedSlice(threadIDX, refrences[index], refrences,ref_base_offset+index*ref_scale);
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = refrences.length *2;
		refrences = Arrays.copyOf(refrences, newSize);
	}
	
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
	public void close() {
		allocator.close();
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