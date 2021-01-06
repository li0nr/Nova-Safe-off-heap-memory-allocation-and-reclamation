package com.yahoo.oak;


import java.nio.ByteBuffer;
import java.util.Arrays;

import sun.misc.Cleaner;
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
	public void add(Long e, int idx) {
		if(size == ArrayOff.length) {
			EnsureCap();
		}

		if(ArrayOff[size]== null)
			ArrayOff[size]= ByteBuffer.allocateDirect(Long.BYTES);
		ArrayOff[size].putLong(e);
		size++;
	}
	
	public long get(int i,int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
			return ArrayOff[i].getLong(0);
	}
	
	public void set(int index, long e,int idx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 ArrayOff[index].putLong(0, e);
	}
	
	public void Delete_Write(int index, long toWrite ,int idx) {
		 if(!this.delete(index)) {
			 ArrayOff[index]=ByteBuffer.allocateDirect(Long.BYTES);
			this.set(index,toWrite, idx);
		 }
	}
	
	public int getSize(){
		return size;
	}
	
   public boolean delete(int index) {
        ByteBuffer removeItem = ArrayOff[index];
        if(removeItem == null ) return false;
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
        ArrayOff[index]=null;
        return true;
    }
   
	private void EnsureCap() {
		int newSize = ArrayOff.length *2;
		ArrayOff = Arrays.copyOf(ArrayOff, newSize);
	}
	
	 @Override
	public void close()  {
		 for (int i=0;i<size;i++){
			 ByteBuffer buffer = ArrayOff[i];
			 if(buffer == null) continue;
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
	 

	static public void main(String args[]) throws InterruptedException{
		OffHeapList list = new OffHeapList(12);
	    for (int i = 0; i < 12; i++) {
	    	list.add((long)i,0);
	    }
	    for (int i = 0; i < 12; i++) {
	    	long x=list.get(i,0);
	    	System.out.println(x);
	    }	  
	    for (int i = 0; i < 6; i++) {
	    	list.delete(i);
	    }
	    for (int i=0; i < 12; i++) {
	    	list.Delete_Write(i, i, 0);
	    }
	}

}
