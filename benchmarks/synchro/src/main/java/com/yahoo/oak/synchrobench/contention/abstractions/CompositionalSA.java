package com.yahoo.oak.synchrobench.contention.abstractions;

public interface CompositionalSA<V> {

    public  boolean fill(final V value, int idx);

    public  boolean ParallelFill(int size);
	
    public  Integer get(int index, int idx);

    public  boolean put(final V value, int index, int idx);
    
    public  boolean remove(int index , int idx);
        
    public long allocated();
    
    public void clear (int size);
    
    public void print();
    
}
