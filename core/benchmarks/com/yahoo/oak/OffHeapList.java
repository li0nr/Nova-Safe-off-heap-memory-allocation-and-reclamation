package com.yahoo.oak;


//import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.lang.reflect.Method;


public class OffHeapList {
	
	
	private static final int DEFAULT_CAPACITY=10;
	
	ByteBuffer[] ArrayOff;
	private int size=0;
	
	
	public OffHeapList(){
		ArrayOff=new ByteBuffer[DEFAULT_CAPACITY];
	}
	
	public void add(int e) {
		if(size == ArrayOff.length) {
			EnsureCap();
		}

		if(ArrayOff[size]== null)
			ArrayOff[size]= ByteBuffer.allocateDirect(Integer.BYTES);
		ArrayOff[size].putInt(e);
		size++;
	}
	
	public int get(int i) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
			return ArrayOff[i].getInt(0);
	}
	
	public void set(int index, int e) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOff[index].putInt(0, e);
	}
	
	public int getSize(){
		return size;
	}
	
   public void remove(int index) {
        ByteBuffer removeItem = ArrayOff[index];

        try {
            Method cleanerMethod = removeItem.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(removeItem);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);
        }catch(Exception e) {
        	e.printStackTrace();
        }



        for (int i = index; i < getSize() - 1; i++) {
        	ArrayOff[i] = ArrayOff[i + 1];
        }
        size--;
    }
	private void EnsureCap() {
		int newSize = ArrayOff.length *2;
		ArrayOff = Arrays.copyOf(ArrayOff, newSize);
	}
	
//	@Test
//	public void NovaListTest() throws InterruptedException{
//	    for (int i = 0; i < 12; i++) {
//	    	this.add(i);
//	    }
//	    for (int i = 0; i < 12; i++) {
//	    	int x=this.get(i);
//	    	System.out.println(x);
//	    }	  
//	    for (int i = 0; i < 12; i++) {
//	    	this.remove(i);
//	    }
//	}

}
