package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class NovaList {

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(1024*1024*1024);
    final NovaManager novaManager = new NovaManager(allocator);
    

	FacadeTransformer<Integer> fread=(ByteBuffer) -> {	return ByteBuffer.getInt(0);  };
	
	
	private FacadeWriteTransformer<Void> serialiaze(int e) {
	    FacadeWriteTransformer<Void> f=(ByteBuffer) -> {	ByteBuffer.putInt(0,e); 
											return null; };
		return f;
		
	}



	Facade[] ArrayOfFacades;
	private int size=0;
	

	
	public NovaList(){
		ArrayOfFacades=new Facade[DEFAULT_CAPACITY];

	}

	public void add(int e) {
		if(size == ArrayOfFacades.length) {
			EnsureCap();
		}

		if(ArrayOfFacades[size]== null)
			ArrayOfFacades[size]=new Facade(novaManager);
			ArrayOfFacades[size].AllocateSlice(4);
	    ArrayOfFacades[size].Write(serialiaze(e));
	    size++;
	}
	
	public int get(int i) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}

		return ArrayOfFacades[i].Read(fread);
	}
	
	public void set(int index, int e) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOfFacades[index].Write(serialiaze(e));
	}
	
	public int getSize(){
		return size;
	}
	
	private void EnsureCap() {
		int newSize = ArrayOfFacades.length *2;
		ArrayOfFacades = Arrays.copyOf(ArrayOfFacades, newSize);
	}
	
	

	

	@Test
	public void NovaListTest() throws InterruptedException{
	    for (int i = 0; i < 12; i++) {
	    	this.add(i);
	    }
	    for (int i = 0; i < 12; i++) {
	    	int x=this.get(i);
	    	System.out.println(x);
	    }	    
	}
	
	
}

