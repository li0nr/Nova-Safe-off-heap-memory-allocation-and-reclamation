package com.yahoo.oak;

import java.util.Comparator;

public interface NovaC<K> extends Comparator<K> {
    default int compare(K key1, K key2) {
        return compareKeys(key1, key2);
    }

    int compareKeys(K key1, K key2);

    int compareKeys(long address, K obj);
    	
    void Print(long address);



}
