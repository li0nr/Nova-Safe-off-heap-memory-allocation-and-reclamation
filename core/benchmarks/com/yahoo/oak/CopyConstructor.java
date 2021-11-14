package com.yahoo.oak;

public interface CopyConstructor<O> {
	public O Copy(O o);
	
	public void overWrite(O o);
}
