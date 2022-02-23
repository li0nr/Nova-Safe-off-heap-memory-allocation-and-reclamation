/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import jdk.incubator.foreign.MemorySegment;
/**
 * An interface to be supported by keys and values provided for Oak's mapping
 */
public interface NovaS<T> {

    // serializes the object
    void serialize(T object, long target);

    void serialize(long source , long target); //off heap seri
    
    void serialize(T object, MemorySegment target); //segment seri

    // deserializes the given Oak buffer    
    T deserialize(long source);

    // returns the number of bytes needed for serializing the given object
    int calculateSize(T object);
}
