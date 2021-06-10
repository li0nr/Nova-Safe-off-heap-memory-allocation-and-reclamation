package com.yahoo.oak;


import java.util.Arrays;
//import org.junit.Test;

import com.yahoo.oak.Facade;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;



public class List_Nova implements ListInterface{

	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager novaManager = new NovaManager(allocator);
    
	



	Facade[] ArrayOfFacades;
	private int size=0;
	

	
	public List_Nova(){
		ArrayOfFacades=new Facade[DEFAULT_CAPACITY];

	}
	
	public List_Nova(int capacity){
		ArrayOfFacades=new Facade[capacity];

	}

	public boolean add(Long e,int idx) {
		if(size == ArrayOfFacades.length) {
			EnsureCap();
		}

		if(ArrayOfFacades[size]== null)
			ArrayOfFacades[size]=new Facade(novaManager);
		ArrayOfFacades[size].AllocateSlice(Long.BYTES,idx);
	    ArrayOfFacades[size].Write(e,idx);
	    size++;
	    return true;
	}
	
	public long get(int i, int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
		return ArrayOfFacades[i].Read();
	}
	
	public boolean set(int index, long e, int idx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		 if(ArrayOfFacades[index].Write(e,idx) != null)
			 return true;
		 else 
			 return false;
	}
	
	public void allocate(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		ArrayOfFacades[index].AllocateSlice(8, threadidx);
	}
	
	public boolean delete(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		return  ArrayOfFacades[index].Delete(threadidx);
	}
	
	
	public int getSize(){
		return size;
	}
	
   public void remove(int index, int idx) {
        Facade removeItem = ArrayOfFacades[index];
        removeItem.Delete(idx);
        
    }
	
	private void EnsureCap() {
		int newSize = ArrayOfFacades.length *2;
		ArrayOfFacades = Arrays.copyOf(ArrayOfFacades, newSize);
	}
	
	public long getUsedMem() {
		return novaManager.allocated();
	}
	
	public long getAllocatedMem() {
		return allocator.numOfAllocatedBlocks()*1024*1024;
	}
	
	
 
@Override
public void close()  {
	novaManager.close();
}
	

public  static void main(String[] args)throws java.io.IOException {
	List_Nova s = new List_Nova();
	for(int i=0; i<100; i++) {
		s.add((long)i,0);
		}
	for(int i=0; i<100; i++) {
		s.set(i,(long)i,0);
		}
	s.close();
	}

}

