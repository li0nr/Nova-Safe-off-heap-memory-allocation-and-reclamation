package com.yahoo.oak;

import java.io.IOException;
import java.util.Arrays;
import com.yahoo.oak.*;
//import org.junit.Test;


public class NovaList implements ListInterface{

	private static final int DEFAULT_CAPACITY=10;
	//final long MEM_CAPACITY=1024;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager novaManager = new NovaManager(allocator);
    

	FacadeReadTransformer<Integer> fread=(ByteBuffer) -> {	return ByteBuffer.getInt(0);  };
	
	
	private FacadeWriteTransformer<Void> serialiaze(long e) {
	    FacadeWriteTransformer<Void> f=(ByteBuffer) -> {	ByteBuffer.putLong(0,e); 
											return null; };
		return f;
		
	}
	
	private FacadeWriteTransformer<Void> f= serialiaze(0);



	Facade[] ArrayOfFacades;
	private int size=0;
	

	
	public NovaList(){
		ArrayOfFacades=new Facade[DEFAULT_CAPACITY];

	}
	
	public NovaList(int capacity){
		ArrayOfFacades=new Facade[capacity];

	}

	public void add(Long e) {
		if(size == ArrayOfFacades.length) {
			EnsureCap();
		}

		if(ArrayOfFacades[size]== null)
			ArrayOfFacades[size]=new Facade(novaManager);
		ArrayOfFacades[size].AllocateSlice(Long.BYTES);
	    ArrayOfFacades[size].Write(f);
	    size++;
	}
	
	public long get(int i) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
		return ArrayOfFacades[i].Read(fread);
	}
	
	public void set(int index, long e) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOfFacades[index].Write(f);
	}
	
	public int getSize(){
		return size;
	}
	
   public void remove(int index) {
        Facade removeItem = ArrayOfFacades[index];
        removeItem.Delete();
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
	

//	@Test
//	public void NovaListTest() throws InterruptedException{
//	    for (int i = 0; i < 12; i++) {
//	    	this.add((long)i);
//	    }
//	    for (int i = 0; i < 12; i++) {
//	    	int x=(int)this.get(i);
//	    	System.out.println(x);
//	    }	  
//	    for (int i = 0; i < 12; i++) {
//	    	this.remove(i);
//	    }
//	}
public  static void main(String[] args)throws java.io.IOException {
	NovaList s = new NovaList();
	//s.RunBenchmark(4, 10, "R", "N");
	for(int i=0; i<10_000_000; i++) {
		s.add((long)i);
	}

	for(int i=0; i<10_000_000; i++) {
		s.set(i,(long)i);
	}
	
	
}	
	
}

