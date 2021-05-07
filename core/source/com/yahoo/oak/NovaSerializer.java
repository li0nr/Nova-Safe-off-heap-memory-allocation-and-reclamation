/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * An interface to be supported by keys and values provided for Oak's mapping
 */
public interface NovaSerializer<T> {

    // serializes the object
    void serialize(T object, long target);
    
    void serialize(T object, HEslice target );
    
    void serialize(T object, NovaSlice target );


    // deserializes the given Oak buffer    
    T deserialize(long source);
    
    T deserialize(HEslice target);

    T deserialize(NovaSlice target);


    // returns the number of bytes needed for serializing the given object
    int calculateSize(T object);
}
