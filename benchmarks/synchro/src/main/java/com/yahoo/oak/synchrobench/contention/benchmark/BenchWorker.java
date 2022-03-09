/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;


import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * A generic benchmark worker.
 * Is contains the common parts of all parallel workers that are used during the benchmark.
 */
public abstract class BenchWorker implements Runnable {

    /**
     * The instance of the running benchmark and key/value generators.
     */
    CompositionalLL bench;


    /**
     * Stores exceptions to later reported by the benchmark.
     */
    protected Exception error = null;

    public BenchWorker(
    		CompositionalLL bench
    ) {
        this.bench = bench;
    }


    /**
     * @param e the exception to be reported
     */
    void reportError(Exception e) {
        error = e;
    }

    /**
     * If en error was reported, the reported exception will be thrown.
     * Otherwise, nothing should happen.
     */
    void getError() throws Exception {
        if (error != null) {
            throw error;
        }
    }
}
