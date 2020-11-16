package com.yahoo.oak;


//import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import sun.misc.Cleaner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;


public class OffHeapList implements ListInterface{
	
	
	private static final int DEFAULT_CAPACITY=10;
	
	ByteBuffer[] ArrayOff;
	private int size=0;
	
	
	public OffHeapList(){
		ArrayOff=new ByteBuffer[DEFAULT_CAPACITY];
	}
	public OffHeapList(int capacity){
		ArrayOff=new ByteBuffer[capacity];

	}
	public void add(Long e) {
		if(size == ArrayOff.length) {
			EnsureCap();
		}

		if(ArrayOff[size]== null)
			ArrayOff[size]= ByteBuffer.allocateDirect(Long.BYTES);
		ArrayOff[size].putLong(e);
		size++;
	}
	
	public long get(int i) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
			return ArrayOff[i].getLong(0);
	}
	
	public void set(int index, long e) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOff[index].putLong(0, 3);
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
	
	 @Override
	public void close()  {
		 for (int i=0;i<size;i++){
			 ByteBuffer buffer = ArrayOff[i];
		        Field cleanerField = null;
		        try {
		            cleanerField = buffer.getClass().getDeclaredField("cleaner");
		        } catch (NoSuchFieldException e) {
		            e.printStackTrace();
		        }
		        assert cleanerField != null;
		        cleanerField.setAccessible(true);
		        Cleaner cleaner = null;
		        try {
		            cleaner = (Cleaner) cleanerField.get(buffer);
		        } catch (IllegalAccessException e) {
		            e.printStackTrace();
		        }
		        assert cleaner != null;
		        cleaner.clean();
		 }
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
