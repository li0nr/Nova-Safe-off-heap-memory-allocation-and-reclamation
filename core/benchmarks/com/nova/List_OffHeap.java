package com.nova;


import java.nio.ByteBuffer;
import java.util.Arrays;


public class List_OffHeap implements ListInterface{
	
	private static final int DEFAULT_CAPACITY=10;
	
	ByteBuffer[] ArrayOff;
	private int size=0;

	
	
	public List_OffHeap(){
		ArrayOff=new ByteBuffer[DEFAULT_CAPACITY];
	}
	public List_OffHeap(int capacity){
		ArrayOff=new ByteBuffer[capacity];
	}
	public boolean add(Long e, int idx) {
		if(size == ArrayOff.length) {
			EnsureCap();
		}

		if(ArrayOff[size]== null)
			ArrayOff[size]= ByteBuffer.allocateDirect(Long.BYTES);
		ArrayOff[size].putLong(e);
		size++;
		return true;
	}
	
	public long get(int i,int idx) {
		if(i>= size || i<0) {
			throw new IndexOutOfBoundsException();
		}
			return ArrayOff[i].getLong(0);
	}
	
	public boolean set(int index, long e,int idx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}

		 if (ArrayOff[index].putLong(0, e)!= null)
			 return true;
		 else 
			 return false;
	}
	
	
	public int getSize(){
		return size;
	}
	public void allocate(int index, int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
		ArrayOff[index]= ByteBuffer.allocateDirect(Long.BYTES);

	}
	
   public boolean delete(int index , int threadidx) {
		if(index>= size || index<0) {
			throw new IndexOutOfBoundsException();
		}
        ByteBuffer removeItem = ArrayOff[index];
        if(removeItem == null ) return false;
//        try {
//            Method cleanerMethod = removeItem.getClass().getMethod("cleaner");
//            cleanerMethod.setAccessible(true);
//            Object cleaner = cleanerMethod.invoke(removeItem);
//            Method cleanMethod = cleaner.getClass().getMethod("clean");
//            cleanMethod.setAccessible(true);
//            cleanMethod.invoke(cleaner);
//        }catch(Exception e) {
//        	e.printStackTrace();
//        }
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
	         }
		 }
	 

	static public void main(String args[]) throws InterruptedException{
		List_OffHeap list = new List_OffHeap(12);
	    for (int i = 0; i < 12; i++) {
	    	list.add((long)i,0);
	    }
	    for (int i = 0; i < 12; i++) {
	    	long x=list.get(i,0);
	    	System.out.println(x);
	    }	  
	}

}
