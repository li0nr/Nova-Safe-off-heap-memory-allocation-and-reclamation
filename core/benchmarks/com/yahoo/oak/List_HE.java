package com.yahoo.oak;

import java.util.Arrays;

import org.openjdk.jmh.runner.RunnerException;

import com.yahoo.oak.HazardEras;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.UnsafeUtils;
import com.yahoo.oak.HazardEras.HEslice;

public class List_HE implements ListInterface{
	
	class DeletedEntry extends RuntimeException {}

	
	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final HazardEras HE = new HazardEras(1, 32, allocator);
	


	private int size=0;
	private volatile HEslice[] Slices;

	
	public List_HE(){
		Slices = new HEslice[DEFAULT_CAPACITY];

	}
	
	public List_HE(int capacity){
		Slices = new HEslice[capacity];

	}

	public boolean add(Long e,int idx) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		if(Slices[size]== null)
			Slices[size] = HE.allocate(Long.BYTES);
		UnsafeUtils.unsafe.putLong(Slices[size].getAddress() + Slices[size].getAllocatedOffset(), e);
	    HE.clear(idx);
		size++;
		return true;
	}
	
	public long get(int index, int idx) {
		if(index >= size || index<0) 
			throw new IndexOutOfBoundsException();
		long x;
		HEslice access = HE.get_protected(Slices[index], 0, idx);
		if(access != null)
			x  = UnsafeUtils.unsafe.getLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset());	
		else {
			HE.clear(idx);
			throw new DeletedEntry();
		}		
		HE.clear(idx);
		return x;	

		}
	

	public boolean set(int index, long e, int idx)  {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		HEslice access = HE.get_protected(Slices[index], 0, idx);
		if(access != null) {
			UnsafeUtils.unsafe.putLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset(),e);
		}
		else {
		    HE.clear(idx);
		    return false;
		}
		HE.clear(idx);
		return true;
		}
	
	public void allocate(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		allocator.allocate(Slices[index], Long.BYTES);

	}
	public boolean delete(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		HEslice toDelete = Slices[index];
		Slices[index]= null;
		if(toDelete != null)
			HE.retire(threadidx, toDelete);
		else return false;
		return  true;

	}
	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	

	
@Override
public void close()  {
	allocator.close();
}
	

public  static void main(String[] args)throws java.io.IOException {

	
	List_HE s = new List_HE();
	for(int i=0; i<100; i++) {
		s.add((long)i,0);
		}
	
	Runnable runnable =
	        () -> { s.delete(4, 1); };
    Runnable runnable1 =
	    	        () -> { s.set(4, 4, 2);};
	    	        
	    	    	Thread write= new Thread(runnable1);
	    	    	Thread delete= new Thread(runnable);
	    	    	write.start();
	    	    	delete.start();
	    	    	
       
	for(int i=0; i<100; i++) {
		s.set(i,(long)i,0);
		}
	s.close();
	}


}
