/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

public interface CompositionalLL<K,V> {

	public Integer containsKey(final K key, int tidx);
	    
    public  boolean Fill(final K key,final V value, int idx);

    public  boolean put(final K key,final V value, int idx);
    
    public  boolean remove(final K key, int idx);
        
    public long allocated();
    
    public void clear ();
    
    public void print();
    
}
