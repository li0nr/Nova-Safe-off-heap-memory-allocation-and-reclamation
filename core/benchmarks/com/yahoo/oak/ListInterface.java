package com.yahoo.oak;

import java.io.Closeable;

public interface ListInterface extends Closeable{

	
	/********needed for Nova threads indx**********/ 
	public void add(Long e,int threadidx);
	
	public long get(int i,int threadidx);
	
	public void set(int index, long e,int threadidx);
			
	public boolean delete(int index, int threadidx);
	
	public void allocate(int index,int threadidx);
	
//	//memory loggers
//	public long getUsedMem();
//	
//	public long getAllocatedMem();
}
