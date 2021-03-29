package com.yahoo.oak;

import java.util.Arrays;

import org.openjdk.jmh.runner.RunnerException;

public class ListHE implements ListInterface{
	
	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final HazardEras HE = new HazardEras(1, 32, allocator);
	


	private int size=0;
	private HEslice[] Slices;

	
	public ListHE(){
		Slices = new HEslice[DEFAULT_CAPACITY];

	}
	
	public ListHE(int capacity){
		Slices = new HEslice[capacity];

	}

	public void add(Long e,int idx) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}

		if(Slices[size]== null)
			Slices[size]=new HEslice(HE.getEra());
		allocator.allocate_otherApproaches(Slices[size], Long.BYTES);
		//HE.get_protected(Slices[size], 1, idx); //needed? -- not measured for now!
		UnsafeUtils.unsafe.putLong(Slices[size].getAddress() + Slices[size].getAllocatedOffset(), e);
	    //HE.clear(1);
		size++;
	}
	
	public long get(int i, int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
			}
		HE.get_protected(Slices[i], 1, idx);
		long x= UnsafeUtils.unsafe.getLong(Slices[i].getAddress() + Slices[i].getAllocatedOffset());
		HE.clear(idx);
		return x;

		}
	

	public void set(int index, long e, int idx)  {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		HEslice access = HE.get_protected(Slices[index], 1, idx);
		if(access != null)
			UnsafeUtils.unsafe.putLong(Slices[index].getAddress() + Slices[index].getAllocatedOffset(),e);
		else {
		    HE.clear(idx);
			throw new DeletedEntry();
		}
		HE.clear(idx);

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
		HE.retire(threadidx, toDelete);
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

	
	ListHE s = new ListHE();
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

class DeletedEntry extends RuntimeException {
	
	
}
class HEslice extends NovaSlice implements HazardEras_interface{
	private long bornEra;
	private long deadEra;
	
	HEslice(long Era){
		super(0,0,0);
		bornEra = Era;
	}
	
	 public void setDeleteEra(long Era){
		 deadEra = Era;
	 }
	 
	 public void setEra(long Era) {
		 bornEra = Era;
	 }

	 public long getnewEra() {
		 return bornEra;
	 }
	 
	 public long getdelEra() {
		 return deadEra;
	 }
	 
	 
	
}
