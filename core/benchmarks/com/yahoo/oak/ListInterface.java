package com.yahoo.oak;

import java.io.Closeable;

public interface ListInterface extends Closeable{

	default public void add(Long e) {}
	
	default public long get(int i) {return 0;}

	default public void set(int index, long e) {}
	
	/********needed for Nova threads indx**********/ 
	default public void add(Long e,int threadidx) {}
	
	default long get(int i,int threadidx) {return 0;}
	

	default public void set(int index, long e,int threadidx) {}
	
		
}
