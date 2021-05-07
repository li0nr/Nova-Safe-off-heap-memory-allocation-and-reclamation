/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;


import java.util.Comparator;

public interface OakComparator<K> extends Comparator<K> {
    default int compare(K key1, K key2) {
        return compareKeys(key1, key2);
    }

    int compareKeys(K key1, K key2);

    
    int compareSerializedKeys(Facade serializedKey1, Facade serializedKey2, int tidx);
    
    int compareKeyAndSerializedKey(K key, Facade serializedKey, int tidx);
    
    
    int compareKeyAndSerializedKey(K key, HEslice serializedKey, int tidx);
    
    int compareSerializedKeys(HEslice serializedKey1, HEslice serializedKey2, int tidx);
    
    
    int compareKeyAndSerializedKey(K key, NovaSlice serializedKey, int tidx);
    
    int compareSerializedKeys(NovaSlice serializedKey1, NovaSlice serializedKey2, int tidx);



}
