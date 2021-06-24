/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Random;

/**
 * The loop executed by each thread of the map
 * benchmark.
 *
 * @author Vincent Gramoli
 */
public class ThreadLoopLL implements Runnable {

    /**
     * The instance of the running benchmark
     */
	CompositionalLL<Buff> bench;
    /**
     * The stop flag, indicating whether the loop is over
     */
    protected volatile boolean stop = false;
    /**
     * The pool of methods that can run
     */
    protected Method[] methods;
    /**
     * The number of the current thread
     */
    protected final short myThreadNum;

    /**
     * The counters of the thread successful operations
     */

    long numAddAll = 0;
    long numRemoveAll = 0;
    long numSize = 0;
    
    long numRemove = 0;
    long numSucRemove = 0;
    
    long numContains = 0;
    long numSucContains = 0;

    long numAdd = 0;
    long numSuccAdd = 0;

    /**
     * The counter of the false-returning operations
     */
    long failures = 0;
    /**
     * The counter of the thread operations
     */
    long total = 0;
    /**
     * The counter of aborts
     */
    long aborts = 0;
    /**
     * The random number
     */
    Random rand = new Random();
    /**
     * iterator operation number
     */
    long iterops = 0;
    
    boolean iterate = false;
    
    Buff key = new Buff(Parameters.confKeySize);

    /**
     * The distribution of methods as an array of percentiles
     * <p>
     * 0%        cdf[0]        cdf[2]                     100%
     * |--writeAll--|--writeSome--|--readAll--|--readSome--|
     * |-----------write----------|--readAll--|--readSome--| cdf[1]
     */
    int[] cdf = new int[3];

    public ThreadLoopLL(short myThreadNum,
                         CompositionalLL<Buff> bench, Method[] methods) {
        this.myThreadNum = myThreadNum;
        this.bench = bench;
        this.methods = methods;
        /* initialize the method boundaries */
        iterate = Parameters.iterate;
        assert (Parameters.confNumWrites >= Parameters.confNumWriteAlls);
        cdf[0] = 10 * Parameters.confNumWriteAlls;
        cdf[1] = 10 * Parameters.confNumWrites;
        cdf[2] = cdf[1] + 10 * Parameters.confNumSnapshots;
    }

    public void stopThread() {
        stop = true;
    }

    public void run() {


        // for the key distribution INCREASING we want to continue the increasing integers sequence,
        // started in the initial filling of the map
        // for the key distribution RANDOM the below value will be overwritten anyway
        int newInt = 0; //start deleting from 0
        //int newInt = Parameters.confSize;

        while (!stop) {
        	
        	if(iterate) {
        		Iterator itr = bench.iterator(myThreadNum);
        		while(itr.hasNext()) {
        			itr.next();
        			total++;
        		}
        		continue;
        	}
        	
            newInt = (Parameters.confKeyDistribution == Parameters.KeyDist.RANDOM) ?
                    rand.nextInt(Parameters.confRange) : newInt + 1;
        	key.set(newInt);

            int coin = rand.nextInt(1000);
            if(coin < cdf[0]) { //-a deleting is good?
        		numRemove++;
            	if(bench.remove(key, myThreadNum)) {
            		numSucRemove++;
            	}
            	else {
            		failures++;
            	}
            }
            else if (coin < cdf[1]) { // -u writing is better than deleting?
            	numAdd++;
            	if(bench.put(key, myThreadNum)) {
            		numSuccAdd++;
            	}
            	else failures++;
            		
            }
            else if (coin < cdf[2]) { // -s reading is the best ever
            	numContains++;
            	if(bench.containsKey(key, myThreadNum)) {
            		numSucContains++;
            	}
            	else {
            		failures++;
            	}
            }
            total++;

            assert total == failures + numContains + + numRemove + numAdd ;
        }
    }

}
