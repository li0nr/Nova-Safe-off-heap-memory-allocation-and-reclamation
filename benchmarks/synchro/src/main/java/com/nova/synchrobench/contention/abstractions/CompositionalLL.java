/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.nova.synchrobench.contention.abstractions;

import com.nova.Buff.Buff;

abstract public interface CompositionalLL<K,V> {

	public Integer containsKey(final K key, int tidx);
	    
    public  boolean putIfAbsent(final K key,final V value, int idx);
    
    public  boolean put(final K key,final V value, int idx);
    
    default public  boolean OverWrite(final Buff key,final Buff value, int idx) 
    {throw new IllegalAccessError();}
    
    public  boolean remove(final K key, int idx);
        
    public long allocated();
    
    public void clear ();
    
    public int size();
    
    public void print();
  
}
