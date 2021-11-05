package com.yahoo.oak.SimpleArray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.yahoo.oak.Facade_Nova;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.SimpleArray.SA_Nova_Primitive_CAS.FillerThread;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak._Global_Defs;
import com.yahoo.oak.Buff.Buff;

public class SA_Nova_CAS {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
    final NovaManager mng = new NovaManager(allocator);
    
	

    private final NovaS srZ;
	private int size=0;
	private Facade_slice[] Slices;

	
	public SA_Nova_CAS(NovaS srZ){
		new Facade_Slice(mng);
		Slices = new Facade_slice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_Nova_CAS(int capacity,NovaS srZ ){
		new Facade_Slice(mng);
		Slices = new Facade_slice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = new Facade_slice();
		Facade_Slice.AllocateSlice(Slices[size],srZ.calculateSize(e), threadIDX);
		Facade_Slice.WriteFast(srZ,e,Slices[size],threadIDX);
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
		return (R)Facade_Slice.Read(Reader,Slices[index]);
	}
	
	public <T> boolean set(int index, T obj, int threadIDX)  {
		Facade_slice toSet = Slices[index];
		if(toSet.isDeleted())
				if(Facade_Slice.AllocateSliceCAS(toSet,srZ.calculateSize(obj),threadIDX))
					return Facade_Slice.WriteFull(srZ, obj, toSet, threadIDX) != null ? true : false;
		return false;	
	}
	
	public boolean delete(int index, int threadIDX) {
		return Facade_Slice.DeleteCAS(threadIDX, Slices[index]);
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
		Facade_slice[] array;
		Random localRanom;
		FillerThread(int index, Facade_slice[] local){
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
				array[idx*1_000_000 + i] = new Facade_slice();
				Facade_Slice.AllocateSlice(Slices[idx*1_000_000 + i],srZ.calculateSize(key), idx);
				Facade_Slice.WriteFast(srZ,key,Slices[idx*1_000_000 + i],idx);
				i++;
				}
			
	        }
	}
}