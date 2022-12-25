package com.nova;

import java.util.Arrays;

import com.nova.NativeMemoryAllocator;
import com.nova.NovaSlice;
import com.nova.UnsafeUtils;

public class List_NoMM implements ListInterface{
	
	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	


	private int size=0;
	private volatile NovaSlice[] Slices;

	
	public List_NoMM(){
		Slices = new NovaSlice[DEFAULT_CAPACITY];

	}
	
	public List_NoMM(int capacity){
		Slices = new NovaSlice[capacity];

	}

	public boolean add(Long e,int idx) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		if(Slices[size]== null)
			Slices[size]=new NovaSlice(0,0,0);
		allocator.allocate(Slices[size], Long.BYTES);
		UnsafeUtils.unsafe.putLong(Slices[size].getAddress() + Slices[size].getAllocatedOffset(), e);
		size++;
		return true;
	}
	
	public long get(int i, int idx) {
		if(i>= size || i<0) 
			throw new IndexOutOfBoundsException();
		long x;
		x  = UnsafeUtils.unsafe.getLong(Slices[i].getAddress() + Slices[i].getAllocatedOffset());	
		return x;	

		}
	

	public boolean set(int index, long e, int idx)  {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		UnsafeUtils.unsafe.putLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset(),e);
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
