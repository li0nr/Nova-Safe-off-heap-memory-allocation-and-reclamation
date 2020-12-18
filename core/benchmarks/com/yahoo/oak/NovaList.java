package com.yahoo.oak;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.oak.*;
//import org.junit.Test;

import sun.misc.Unsafe;


public class NovaList implements ListInterface{

	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager novaManager = new NovaManager(allocator);
    
	



	Facade[] ArrayOfFacades;
	private int size=0;
	

	
	public NovaList(){
		ArrayOfFacades=new Facade[DEFAULT_CAPACITY];

	}
	
	public NovaList(int capacity){
		ArrayOfFacades=new Facade[capacity];

	}

	public void add(Long e,int idx) {
		if(size == ArrayOfFacades.length) {
			EnsureCap();
		}

		if(ArrayOfFacades[size]== null)
			ArrayOfFacades[size]=new Facade(novaManager);
		ArrayOfFacades[size].AllocateSlice(Long.BYTES,idx);
	    ArrayOfFacades[size].Write(e,idx);
	    size++;
	}
	
	public long get(int i, int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
		return ArrayOfFacades[i].Read();
	}
	
	public void set(int index, long e, int idx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOfFacades[index].Write(e,idx);
	}
	
	public int getSize(){
		return size;
	}
	
   public void remove(int index, int idx) {
        Facade removeItem = ArrayOfFacades[index];
        removeItem.Delete(idx);
        for (int i = index; i < getSize() - 1; i++) {
        	ArrayOfFacades[i] = ArrayOfFacades[i + 1];
        }
        size--;
    }
	
	private void EnsureCap() {
		int newSize = ArrayOfFacades.length *2;
		ArrayOfFacades = Arrays.copyOf(ArrayOfFacades, newSize);
	}
	
	
 
@Override
public void close()  {
	novaManager.close();
}
	

public  static void main(String[] args)throws java.io.IOException {
	NovaList s = new NovaList();
	for(int i=0; i<100; i++) {
		s.add((long)i);
		}
	for(int i=0; i<100; i++) {
		s.set(i,(long)i);
		}
	s.close();
	}

}

