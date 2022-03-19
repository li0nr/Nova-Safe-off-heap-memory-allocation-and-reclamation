package com.yahoo.oak;

import java.util.Comparator;

import jdk.incubator.foreign.MemorySegment;


public interface NovaC<K> extends Comparator<K> {
    default int compare(K key1, K key2) {
        return compareKeys(key1, key2);
    }

    int compareKeys(K key1, K key2);

    int compareKeys(long address, K obj);
    
    default int compareKeys(MemorySegment address, K obj) {
    	throw new RuntimeException();
    }
    
    int compareKeys(long address, long address2);

    	
    void Print(long address);



}
