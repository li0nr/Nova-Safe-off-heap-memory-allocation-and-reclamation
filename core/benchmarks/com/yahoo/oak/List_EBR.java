package com.yahoo.oak;

import java.util.Arrays;

import org.openjdk.jmh.runner.RunnerException;

public class List_EBR implements ListInterface{
	
	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final EBR<EBRslice> EBR = new EBR(32, allocator);
	


	private int size=0;
	private volatile EBRslice[] Slices;

	
	public List_EBR(){
		Slices = new EBRslice[DEFAULT_CAPACITY];

	}
	
	public List_EBR(int capacity){
		Slices = new EBRslice[capacity];

	}

	public boolean add(Long e,int idx) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		if(Slices[size]== null)
			Slices[size]=new EBRslice(EBR.getEpoch());
		allocator.allocate(Slices[size], Long.BYTES);
		UnsafeUtils.unsafe.putLong(Slices[size].getAddress() + Slices[size].getAllocatedOffset(), e);
	    EBR.clear(idx);
		size++;
		return true;
	}
	
	public long get(int index, int idx) {
		if(index >= size || index<0) 
			throw new IndexOutOfBoundsException();
		EBR.start_op(idx);
		long x  ;
		if(Slices[index ]!= null)
			x= UnsafeUtils.unsafe.getLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset());	
		else {
			EBR.clear(idx);
			throw new RuntimeException("not found");
		}		
		EBR.clear(idx);
		return x;	
	}
	

	public boolean set(int index, long e, int idx)  {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		EBR.start_op(idx);
		if(Slices[index] != null)
			UnsafeUtils.unsafe.putLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset(),e);
		else {
			EBR.clear(idx);
			return false;
		}
		EBR.clear(idx);
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
		EBR.start_op(threadidx);
		EBRslice toDelete = Slices[index];
		Slices[index]= null;
		if(toDelete != null)
			EBR.retire(toDelete , threadidx);
		else {
			EBR.clear(threadidx);
			return false;
		}
		EBR.clear(threadidx);
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

	
	List_EBR s = new List_EBR();
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

class EBRslice extends NovaSlice implements EBR_interface{
	private long bornEra;
	
	EBRslice(long Era){
		super(0,0,0);
		bornEra = Era;
	}

	public void setEpoch(long Era){
		 bornEra = Era;
	 }

	 public long geteEpoch() {
		 return bornEra;
	 }
	 
}