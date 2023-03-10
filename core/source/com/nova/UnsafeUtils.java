/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.nova;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;

public final class UnsafeUtils {

    public static Unsafe unsafe;

    // static constructor - access and create a new instance of Unsafe
    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private UnsafeUtils() {
    }

    static final long LONG_INT_MASK = (1L << Integer.SIZE) - 1L;

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     */
    static long intsToLong(int i1, int i2) {
        return (i1 & LONG_INT_MASK) | (((long) i2) << Integer.SIZE);
    }
    
    public static void putInt(long address, int value) {
    	unsafe.putInt(address, value);
    }
    
    public static int getInt(long address) {
    	return unsafe.getInt(address);
    }
    
    public static void putLong(long address, long value) {
    	unsafe.putLong(address, value);
    }
    
    public static long getLong(long address) {
    	return unsafe.getLong(address);
    }
}
