package com.yahoo.oak;

import java.io.Closeable;

public interface ListInterface extends Closeable{

	
	/********needed for Nova threads indx**********/ 
	public void add(Long e,int threadidx);
	
	public long get(int i,int threadidx);
	

	public void set(int index, long e,int threadidx);
	
	public void Delete_Write(int toDelete, long toWrite ,int threadidx);
		
}
