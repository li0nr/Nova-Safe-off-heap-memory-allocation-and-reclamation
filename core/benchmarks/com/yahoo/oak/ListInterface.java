package com.yahoo.oak;

import java.io.Closeable;

public interface ListInterface extends Closeable{

	public void add(Long e);
	
	public long get(int i) ;

	public void set(int index, long e) ;
	
	public int getSize();
	
	
}
