/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;


import java.util.Random;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;



/**
 * A worker that is used to fill the map concurrently before the benchmarks starts.
 */
public class FillWorker extends BenchWorker {

    /**
     * The number of elements to fill.
     */
    final long size;
    final long range;

    /**
     * The number of operation performed by this worker.
     */
    long operations = 0;
    
    int index = -1;

    public FillWorker(
        CompositionalLL bench,
        long range,
        long size,
        int index
    ) {
        super(bench);
        this.range = range;
        this.size = size;
        this.index = index;
    }

    public long getOperations() {
        return operations;
    }

    @Override
    public void run() {
        try {
            fill();
        } catch (Exception e) {
            System.err.printf("Failed during initial fill: %s%n", e.getMessage());
            reportError(e);
        }
    }

    public void fill() {
        final Random localRand =  new Random();
    	int v = 0;

        for (int i = 0; i < size; ) {
            v = (Parameters.confKeyDistribution == Parameters.KeyDist.INCREASING)
                    ? v + 1 : localRand.nextInt((int)range);
            Buff key = new Buff(Parameters.confKeySize);
            key.set(v);
            Buff val = new Buff(Parameters.confValSize);
            val.set(v+1);

            if (bench.putIfAbsent(key, val, index)) {
                i++;
            }
            // counts all the putIfAbsent operations, not only the successful ones
            operations++;
        }
    }
}
