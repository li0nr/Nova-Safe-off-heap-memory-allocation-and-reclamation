/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.benchmark;

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalBST;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalSA;
import com.yahoo.oak.synchrobench.contention.abstractions.MaintenanceAlg;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Formatter;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.runners.parameterized.ParametersRunnerFactory;

/**
 * Synchrobench-java, a benchmark to evaluate the implementations of
 * high level abstractions including Map and Set.
 *
 * @author Vincent Gramoli
 */
public class Test {

    public enum Type {
        LL,
        SA
    }

    /**
     * The array of threads executing the benchmark
     */
    private Thread[] threads;
    /**
     * The array of runnable thread codes
     */
    private ThreadLoopLL[] threadLoops_LL;
    private ThreadLoopSA[] threadLoops_SA;


    /**
     * The observed duration of the benchmark
     */
    private double elapsedTime;
    /**
     * The throughput
     */
    private double[] throughput = null;
    /**
     * Element count
     */
    private int[] totalSize = null;
    /**
     * The iteration
     */
    private int currentIteration = 0;

    /**
     * The total number of operations for all threads
     */
    private long total = 0;
    /**
     * The total number of successful operations for all threads
     */
    private long numAdd = 0;
    private long numRemove = 0;
    private long numContains = 0;
    
    private long numSucAdd = 0;
    private long numSucRemove = 0;
    private long numSucContains = 0;

    
    private long numAddAll = 0;
    private long numRemoveAll = 0;
    private long numSize = 0;
    /**
     * The total number of failed operations for all threads
     */
    private long failures = 0;
    /**
     * The total number of aborts
     */
    private long aborts = 0;
    
    private long iterOps = 0;
    /**
     * The instance of the benchmark
     */
    private Type benchType = null;
    private CompositionalLL<Buff,Buff> LL = null;
    private CompositionalSA<Buff> SA = null;

    /**
     * The benchmark methods
     */
    private Method[] methods;

    private long nodesTraversed;
    private long structMods;
    private long getCount;

    /**
     * The thread-private PRNG
     */
    private static final ThreadLocal<Random> S_RANDOM = new ThreadLocal<Random>() {
        @Override
        protected synchronized Random initialValue() {
            return new Random();
        }
    };

    public void fill(final long range, final long size) throws InterruptedException {
        // Non-random key distribution can only be initialized from one thread.
        final int numWorkers = Parameters.isRandomKeyDistribution() ? Parameters.confNumFillThreads : 1;
        FillWorker[] fillWorkers = new FillWorker[numWorkers];
        Thread[] fillThreads = new Thread[numWorkers];
        final long sizePerThread = size / numWorkers;
        final long reminder = size % numWorkers;
        for (int i = 0; i < numWorkers; i++) {
            final long sz = i < reminder ? sizePerThread + 1 : sizePerThread;
            fillWorkers[i] = new FillWorker(LL,range, sz, i);
            fillThreads[i] = new Thread(fillWorkers[i]);
        }
        final long reportGranMS = 200;
        final boolean isConsole = System.console() != null;

        System.out.print("Start filling data...");
        if (!isConsole) {
            System.out.println();
        }

        final long startTime = System.currentTimeMillis();
        for (Thread thread : fillThreads) {
            thread.start();
        }

        try {
            if (isConsole) {
                while ((LL.size() < size) && Stream.of(fillThreads).anyMatch(Thread::isAlive)) {
                    long operations = Stream.of(fillWorkers).mapToLong(FillWorker::getOperations).sum();
                    final long curTime = System.currentTimeMillis();
                    double runTime = ((double) (curTime - startTime)) / 1000.0;
                    System.out.printf(
                        "\rFilling data: %5.0f%% -- %,6.2f (seconds) - %,d operations",
                        (float) LL.size() * 100 / (float) size,
                        runTime,
                        operations
                    );
                    Thread.sleep(reportGranMS);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("\nFilling was interrupted. Waiting to finish.");
        } finally {
            for (Thread t : fillThreads) {
                t.join();
            }
        }
        final long endTime = System.currentTimeMillis();
        double initTime = ((double) (endTime - startTime)) / 1000.0;
        long operations = Stream.of(fillWorkers).mapToLong(FillWorker::getOperations).sum();

        if (isConsole) {
            System.out.print("\r");
        }
        System.out.printf("Initialization complete in %,.4f (seconds) - %,d operations%n", initTime, operations);
    }


    /**
     * Instantiate abstraction
     */
    @SuppressWarnings("unchecked")
    public void instanciateAbstraction(String benchName) {
        try {
        	Class benchClass; Constructor c;
        	if(!benchName.contains("SA")) {
                    benchClass = (Class<CompositionalBST<Buff, Buff>>) Class
                            .forName(benchName);
                    Class[] cArg = new Class[1];
                    cArg[0] = Long.TYPE;
                    c = benchClass.getDeclaredConstructor(cArg);	
        	}
        	else {
                benchClass = (Class<CompositionalSA<Buff>>) Class
                        .forName(benchName);
                Class[] cArg = new Class[1];
                cArg[0] = Integer.TYPE;
                c = benchClass.getDeclaredConstructor(cArg);
        	}
        	if (CompositionalLL.class.isAssignableFrom((Class<?>) benchClass)) {
            	if(Parameters.offheap != -1)
                	LL = (CompositionalLL<Buff,Buff>) c.newInstance(Parameters.offheap);
            	else 
                	LL = (CompositionalLL<Buff,Buff>) c.newInstance(0);
    			benchType = Type.LL;
            } else if (CompositionalSA.class.isAssignableFrom((Class<?>) benchClass)) {
            	SA = (CompositionalSA<Buff>) c.newInstance(Parameters.confSize);
    			benchType = Type.SA;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * Creates as many threads as requested
     *
     * @throws InterruptedException if unable to launch them
     */
    private void initThreads() throws InterruptedException {
        switch (benchType) {
            case LL:
            	threadLoops_LL = new ThreadLoopLL[Parameters.confNumThreads];
                threads = new Thread[Parameters.confNumThreads];
                for (short threadNum = 0; threadNum < Parameters.confNumThreads; threadNum++) {
                	threadLoops_LL[threadNum] = new ThreadLoopLL(threadNum, LL, methods);
                    threads[threadNum] = new Thread(threadLoops_LL[threadNum]);
                }
                break;
            case SA:
            	threadLoops_SA = new ThreadLoopSA[Parameters.confNumThreads];
                threads = new Thread[Parameters.confNumThreads];
                for (short threadNum = 0; threadNum < Parameters.confNumThreads; threadNum++) {
                	threadLoops_SA[threadNum] = new ThreadLoopSA(threadNum, SA, methods);
                    threads[threadNum] = new Thread(threadLoops_SA[threadNum]);
                }
                break;
        }
    }

    /**
     * Constructor sets up the benchmark by reading parameters and creating
     * threads
     *
     * @param args the arguments of the command-line
     */
    public Test(String[] args) {
        printHeader();
        try {
            parseCommandLineParameters(args);
        } catch (Exception e) {
            System.err.println("Cannot parse parameters.");
            e.printStackTrace();
        }
        instanciateAbstraction(Parameters.confBenchClassName);
        this.throughput = new double[Parameters.confIterations];
        this.totalSize = new int[Parameters.confIterations];
    }

    private void printHeapStats(String message) {
        System.gc();
        float heapSize = Runtime.getRuntime().totalMemory(); // Get current size of heap in bytes
        float heapFreeSize = Runtime.getRuntime().freeMemory();
        final float gb = (float) (1L << 30);

        float allocated = Float.NaN;
        try {
        	switch (benchType) {
			case LL:
	        	allocated = ( LL).allocated();
				break;
			case SA:
	        	allocated = ( SA).allocated();
			default:
				break;
			}
        } catch (ClassCastException ignored) {
            System.out.println("Cannot fetch off-heap stats for non-Oak maps.");
        }

        System.out.println(); // Extra line space
        System.out.println(message);
        System.out.format("      Heap Total: %.4f GB\n",  heapSize / gb);
        System.out.format("      Heap Usage: %.4f GB\n", (heapSize - heapFreeSize) / gb);
        System.out.format("  Off-Heap Usage: %.4f GB\n", allocated / gb);
    }

    /**
     * Execute the main thread that starts and terminates the benchmark threads
     *
     * @throws InterruptedException
     */
    private void execute(int milliseconds, boolean maint)
            throws InterruptedException {
        printHeapStats("Before initial fill");
        long startTime = System.currentTimeMillis();
        fill(Parameters.confRange, Parameters.confSize);
        double initTime = ((double) (System.currentTimeMillis() - startTime)) / 1000.0;
        printHeapStats("After initial fill, before benchmark");

//        Thread.sleep(5000);
        startTime = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            if(Parameters.memory) {
            	NativeMemoryAllocator.collectStats();
            	int total_sleepy = 0;
            	while(total_sleepy < milliseconds) {
                	total_sleepy += 10000;
                	Thread.sleep(10000);
                	printHeapStats("after" +total_sleepy/1000+ "secs\n");
                	NativeMemoryAllocator.PrintStats();
            	}        	
            }
            else 
                Thread.sleep(milliseconds);
        } finally {
        	switch (benchType) {
			case LL:
        		for (ThreadLoopLL threadLoop : threadLoops_LL) {
        			threadLoop.stopThread();
        			}
				break;
			case SA:
        		for (ThreadLoopSA threadLoop : threadLoops_SA) {
        			threadLoop.stopThread();
        			}
				break;

			default:
				break;
			}

        		}
        for (Thread thread : threads) {
            thread.join();
        }

        long endTime = System.currentTimeMillis();
        elapsedTime = ((double) (endTime - startTime)) / 1000.0;
        printHeapStats("After benchmark");
    }

    public void clear() {
        switch (benchType) {
            case LL:
            	LL.clear();
            	break;
            case SA:
            	SA.clear(Parameters.confSize);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        boolean firstIteration = true;
        Test test = new Test(args);
        test.printParams();

        // warming up the JVM
        if (Parameters.confWarmUp != 0) {
            try {
                test.initThreads();
            } catch (Exception e) {
                System.err.println("Cannot launch operations.");
                e.printStackTrace();
            }
            test.execute(Parameters.confWarmUp * 1000, true);
            // give time to the JIT
            Thread.sleep(1000);
            if (Parameters.confDetailedStats) {
                test.recordPreliminaryStats();
            }
            test.clear();
            test.resetStats();
            System.out.println("Warmup complete");
        }

        // running the bench
        for (int i = 0; i < Parameters.confIterations; i++) {
            if (!firstIteration) {
                // give time to the JIT
                Thread.sleep(1000);
                test.resetStats();
                test.clear();
            }
            try {
                test.initThreads();
            } catch (Exception e) {
                System.err.println("Cannot launch operations.");
                e.printStackTrace();
            }
            test.execute(Parameters.confNumMilliseconds, false);

            test.printBasicStats();
            if (Parameters.confDetailedStats) {
                test.printDetailedStats();
               //F ( test.BST).printMemStats();
            }

            firstIteration = false;
            test.currentIteration++;
        }

        if (Parameters.confIterations > 1) {
            test.printIterationStats();
        }
    }

    /* ---------------- Input/Output -------------- */

    /**
     * Parse the parameters on the command line
     */
    private void parseCommandLineParameters(String[] args) throws Exception {
        int argNumber = 0;

        while (argNumber < args.length) {
            String currentArg = args[argNumber++];

            try {
                switch (currentArg) {
                    case "--help":
                    case "-h":
                        printUsage();
                        System.exit(0);
                    case "--verbose":
                    case "-e":
                        Parameters.confDetailedStats = true;
                        break;
                    case "--change":
                    case "-c":
                        Parameters.confChange = true;
                        break;
                    case "--stream-iteration":
                    case "-si":
                        Parameters.confStreamIteration = true;
                        break;
                    case "--inc":
                        Parameters.confKeyDistribution = Parameters.KeyDist.INCREASING;
                        break;
                    case "--thread-nums":
                    case "-t":
                        Parameters.confNumThreads = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--duration":
                    case "-d":
                        Parameters.confNumMilliseconds = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--updates":
                    case "-u":
                        Parameters.confNumWrites = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--writeAll":
                    case "-a":
                        Parameters.confNumWriteAlls = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--snapshots":
                    case "-s":
                        Parameters.confNumSnapshots = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--size":
                    case "-i":
                        Parameters.confSize = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--range":
                    case "-r":
                        Parameters.confRange = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--fill-threads":
                        Parameters.confNumFillThreads = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--Warmup":
                    case "-W":
                        Parameters.confWarmUp = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--benchmark":
                    case "-b":
                        Parameters.confBenchClassName = args[argNumber++];
                        break;
                    case "--iterations":
                    case "-n":
                        Parameters.confIterations = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--keySize":
                    case "-k":
                        Parameters.confKeySize = Integer.parseInt(args[argNumber++]);
                        break;
                    case "--valSize":
                    case "-v":
                        Parameters.confValSize = Integer.parseInt(args[argNumber++]);
                        break;
                    case "-itr":
                        Parameters.iterate = true;
                        break;
                    case "-m":
                        Parameters.memory = true;
                        break;
                    case "-o":
                        Parameters.offheap = (Integer.parseInt(args[argNumber++])) * (long) (1024*1024*1024);
                        break;
                    case "-par":
                        Parameters.parallelFill = true;
                        break;
                    case "-oW":
                        Parameters.overWrite = true;
                        break;
                }
            } catch (IndexOutOfBoundsException e) {
                System.err.println("Missing value after option: " + currentArg + ". Ignoring...");
            } catch (NumberFormatException e) {
                System.err.println("Number expected after option: " + currentArg + ". Ignoring...");
            }
        }
        assert (Parameters.confRange >= Parameters.confSize);
        if (Parameters.confRange != 2 * Parameters.confSize) {
            System.err.println("Note that the value range is not twice the initial size, thus the size " +
                    "expectation varies at runtime.");
        }

    }

    /**
     * Print a 80 character line filled with the same marker character
     *
     * @param ch the marker character
     */
    private void printLine(char ch) {
        StringBuffer line = new StringBuffer(79);
        for (int i = 0; i < 79; i++) {
            line.append(ch);
        }
        System.out.println(line);
    }

    /**
     * Print the header message on the standard output
     */
    private void printHeader() {
        String header = "Synchrobench-java\n"
                + "A benchmark-suite to evaluate synchronization techniques";
        printLine('-');
        System.out.println(header);
        printLine('-');
        System.out.println();
    }

    /**
     * Print the benchmark usage on the standard output
     */
    private void printUsage() {
        String syntax = "Usage:\n"
                + "java synchrobench.benchmark.Test [options] [-- stm-specific options]\n\n"
                + Parameters.asString();
        System.err.println(syntax);
    }

    /**
     * Print the parameters that have been given as an input to the benchmark
     */
    private void printParams() {
        String params = "Benchmark parameters" + "\n" + "--------------------"
                + "\n" + "  Detailed stats:          \t"
                + (Parameters.confDetailedStats ? "enabled" : "disabled")
                + "\n"
                + "  Number of threads:       \t"
                + Parameters.confNumThreads
                + "\n"
                + "  Length:                  \t"
                + Parameters.confNumMilliseconds
                + " ms\n"
                + "  Write ratio:             \t"
                + Parameters.confNumWrites
                + " %\n"
                + "  WriteAll ratio:          \t"
                + Parameters.confNumWriteAlls
                + " %\n"
                + "  Snapshot ratio:          \t"
                + Parameters.confNumSnapshots
                + " %\n"
                + "  Size:                    \t"
                + Parameters.confSize
                + " elts\n"
                + "  Range:                   \t"
                + Parameters.confRange
                + " elts\n"
                + "  WarmUp:                  \t"
                + Parameters.confWarmUp
                + " s\n"
                + "  Iterations:              \t"
                + Parameters.confIterations
                + "\n"
                + "  Key size:              \t"
                + Parameters.confKeySize
                + " Bytes\n"
                + "  Val size:              \t"
                + Parameters.confValSize
                + " Bytes\n"
                + "  Change:                \t"
                + Parameters.confChange
                + "\n"
                + "  Benchmark:               \t"
                + Parameters.confBenchClassName;
        System.out.println(params);
    }

    /**
     * Print the statistics on the standard output
     */
    private void printBasicStats() {
        for (short threadNum = 0; threadNum < Parameters.confNumThreads; threadNum++) {
        	switch (benchType) {
			case LL:
	        	numAdd += threadLoops_LL[threadNum].numAdd;
	            numRemove += threadLoops_LL[threadNum].numRemove;
	            numContains += threadLoops_LL[threadNum].numContains;

	            numSucAdd += threadLoops_LL[threadNum].numSuccAdd;
	            numSucRemove += threadLoops_LL[threadNum].numSucRemove;
	            numSucContains += threadLoops_LL[threadNum].numSucContains;

	            failures += threadLoops_LL[threadNum].failures;
	            total += threadLoops_LL[threadNum].total;
	            iterOps += threadLoops_LL[threadNum].itrSuccess;
				break;
				
			case SA:
	        	numAdd += threadLoops_SA[threadNum].numAdd;
	            numRemove += threadLoops_SA[threadNum].numRemove;
	            numContains += threadLoops_SA[threadNum].numContains;

	            numSucAdd += threadLoops_SA[threadNum].numSuccAdd;
	            numSucRemove += threadLoops_SA[threadNum].numSucRemove;
	            numSucContains += threadLoops_SA[threadNum].numSucContains;

	            failures += threadLoops_SA[threadNum].failures;
	            total += threadLoops_SA[threadNum].total;
	            iterOps += threadLoops_SA[threadNum].itrSuccess;
				break;

			default:
				break;
			}
        }
        throughput[currentIteration] = ((double) total / elapsedTime);
        //totalSize[currentIteration] = oakBench.size();
        printLine('-');
        System.out.println("Benchmark statistics");
        printLine('-');
        //System.out.println("  Average traversal length: \t"
          //      + (double) nodesTraversed / (double) getCount);
        //System.out.println("  Struct Modifications:     \t" + structMods);
        System.out.println("  Throughput (ops/s):       \t" + throughput[currentIteration]);
        System.out.println("  Elapsed time (s):         \t" + elapsedTime);
        System.out.println("  Operations:               \t" + total
                + "\t( 100 %)");
        System.out
                .println("    effective updates:     \t"
                        + (numAdd + numRemove + numAddAll + numRemoveAll)
                        + "\t( "
                        + formatDouble(((double) (numAdd + numRemove
                        + numAddAll + numRemoveAll) * 100)
                        / (double) total) + " %)");
        System.out.println("    |--add all:     	\t" + numAdd + "\t( "
                + formatDouble(((double) numAdd / (double) total) * 100)
                + " %)");
        System.out.println("    |--remove all.:       \t" + numRemove + "\t( "
                + formatDouble(((double) numRemove / (double) total) * 100)
                + " %)");
//        System.out.println("    |--addAll succ.:       \t" + numAddAll + "\t( "
//                + formatDouble(((double) numAddAll / (double) total) * 100)
//                + " %)");
        System.out.println("    iter succ.:    \t" + iterOps
                + "\t( "
                + formatDouble(((double) iterOps / (double) total) * 100)
                + " %)");
        System.out.println("    contains all:        \t" + numContains
                + "\t( "
                + formatDouble(((double) numContains / (double) total) * 100)
                + " %)");
        System.out.println("    **contains succ.:        \t" + numSucContains
                + "\t( "
                + formatDouble(((double) numSucContains / (double) total) * 100)
                + " %)");
        System.out.println("    **add succ.:	     \t" + numSucAdd+ "\t( "
                + formatDouble(((double) numSucAdd / (double) total) * 100)
                + " %)");
        System.out.println("    **remove succ.:       \t" + numSucRemove + "\t( "
                + formatDouble(((double) numSucRemove / (double) total) * 100)
                + " %)");

        System.out.println("    unsuccessful ops:      \t" + failures + "\t( "
                + formatDouble(((double) failures / (double) total) * 100)
                + " %)");
    }

    /**
     * Detailed Warmup TM Statistics
     */
    private int numCommits = 0;
    private int numStarts = 0;
    private int numAborts = 0;

    private int numCommitsReadOnly = 0;
    private int numCommitsElastic = 0;
    private int numCommitsUpdate = 0;

    private int numAbortsBetweenSuccessiveReads = 0;
    private int numAbortsBetweenReadAndWrite = 0;
    private int numAbortsExtendOnRead = 0;
    private int numAbortsWriteAfterRead = 0;
    private int numAbortsLockedOnWrite = 0;
    private int numAbortsLockedBeforeRead = 0;
    private int numAbortsLockedBeforeElasticRead = 0;
    private int numAbortsLockedOnRead = 0;
    private int numAbortsInvalidCommit = 0;
    private int numAbortsInvalidSnapshot = 0;

    private double readSetSizeSum = 0.0;
    private double writeSetSizeSum = 0.0;
    private int statSize = 0;
    private int txDurationSum = 0;
    private int elasticReads = 0;
    private int readsInROPrefix = 0;

    /**
     * This method is called between two runs of the benchmark within the same
     * JVM to enable its warmup
     */
    public void resetStats() {

        for (short threadNum = 0; threadNum < Parameters.confNumThreads; threadNum++) {
        	switch (benchType) {
			case LL:
	        	threadLoops_LL[threadNum].numAdd = 0;
	        	threadLoops_LL[threadNum].numRemove = 0;
	        	threadLoops_LL[threadNum].numAddAll = 0;
	            threadLoops_LL[threadNum].numRemoveAll = 0;
	            threadLoops_LL[threadNum].numSize = 0;
	            threadLoops_LL[threadNum].numContains = 0;
	            threadLoops_LL[threadNum].failures = 0;
	            threadLoops_LL[threadNum].total = 0;
	            threadLoops_LL[threadNum].aborts = 0;
	            threadLoops_LL[threadNum].numSuccAdd = 0;
	            threadLoops_LL[threadNum].numSucContains = 0;
	            threadLoops_LL[threadNum].numSucRemove = 0;
	            threadLoops_LL[threadNum].itrSuccess = 0;
				break;
			case SA:
	        	threadLoops_SA[threadNum].numAdd = 0;
	        	threadLoops_SA[threadNum].numRemove = 0;
	        	threadLoops_SA[threadNum].numAddAll = 0;
	        	threadLoops_SA[threadNum].numRemoveAll = 0;
	        	threadLoops_SA[threadNum].numSize = 0;
	        	threadLoops_SA[threadNum].numContains = 0;
	        	threadLoops_SA[threadNum].failures = 0;
	        	threadLoops_SA[threadNum].total = 0;
	        	threadLoops_SA[threadNum].aborts = 0;
	            threadLoops_SA[threadNum].numSuccAdd = 0;
	            threadLoops_SA[threadNum].numSucContains = 0;
	            threadLoops_SA[threadNum].numSucRemove = 0;
	            threadLoops_SA[threadNum].itrSuccess = 0;
				break;

			default:
				break;
			}
        }
        numAdd = 0;
        numRemove = 0;
        numAddAll = 0;
        numRemoveAll = 0;
        numSize = 0;
        numContains = 0;
        failures = 0;
        total = 0;
        aborts = 0;
        nodesTraversed = 0;
        getCount = 0;
        structMods = 0;

        numCommits = 0;
        numStarts = 0;
        numAborts = 0;
        
        numSucAdd=0;
        numSucContains=0;
        numSucRemove=0;
        
        numCommitsReadOnly = 0;
        numCommitsElastic = 0;
        numCommitsUpdate = 0;

        numAbortsBetweenSuccessiveReads = 0;
        numAbortsBetweenReadAndWrite = 0;
        numAbortsExtendOnRead = 0;
        numAbortsWriteAfterRead = 0;
        numAbortsLockedOnWrite = 0;
        numAbortsLockedBeforeRead = 0;
        numAbortsLockedBeforeElasticRead = 0;
        numAbortsLockedOnRead = 0;
        numAbortsInvalidCommit = 0;
        numAbortsInvalidSnapshot = 0;

        readSetSizeSum = 0.0;
        writeSetSizeSum = 0.0;
        statSize = 0;
        txDurationSum = 0;
        elasticReads = 0;
        readsInROPrefix = 0;
    }

    public void recordPreliminaryStats() {
        numAborts = Statistics.getTotalAborts();
        numCommits = Statistics.getTotalCommits();
        numCommitsReadOnly = Statistics.getNumCommitsReadOnly();
        numCommitsElastic = Statistics.getNumCommitsElastic();
        numCommitsUpdate = Statistics.getNumCommitsUpdate();
        numStarts = Statistics.getTotalStarts();
        numAbortsBetweenSuccessiveReads = Statistics
                .getNumAbortsBetweenSuccessiveReads();
        numAbortsBetweenReadAndWrite = Statistics
                .getNumAbortsBetweenReadAndWrite();
        numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead();
        numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead();
        numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite();
        numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead();
        numAbortsLockedBeforeElasticRead = Statistics
                .getNumAbortsLockedBeforeElasticRead();
        numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead();
        numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit();
        numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot();
        readSetSizeSum = Statistics.getSumReadSetSize();
        writeSetSizeSum = Statistics.getSumWriteSetSize();
        statSize = Statistics.getStatSize();
        txDurationSum = Statistics.getSumCommitingTxTime();
        elasticReads = Statistics.getTotalElasticReads();
        readsInROPrefix = Statistics.getTotalReadsInROPrefix();
    }

    /**
     * Print the detailed statistics on the standard output
     */
    private void printDetailedStats() {

        numCommits = Statistics.getTotalCommits() - numCommits;
        numStarts = Statistics.getTotalStarts() - numStarts;
        numAborts = Statistics.getTotalAborts() - numAborts;

        numCommitsReadOnly = Statistics.getNumCommitsReadOnly()
                - numCommitsReadOnly;
        numCommitsElastic = Statistics.getNumCommitsElastic()
                - numCommitsElastic;
        numCommitsUpdate = Statistics.getNumCommitsUpdate() - numCommitsUpdate;

        numAbortsBetweenSuccessiveReads = Statistics
                .getNumAbortsBetweenSuccessiveReads()
                - numAbortsBetweenSuccessiveReads;
        numAbortsBetweenReadAndWrite = Statistics
                .getNumAbortsBetweenReadAndWrite()
                - numAbortsBetweenReadAndWrite;
        numAbortsExtendOnRead = Statistics.getNumAbortsExtendOnRead()
                - numAbortsExtendOnRead;
        numAbortsWriteAfterRead = Statistics.getNumAbortsWriteAfterRead()
                - numAbortsWriteAfterRead;
        numAbortsLockedOnWrite = Statistics.getNumAbortsLockedOnWrite()
                - numAbortsLockedOnWrite;
        numAbortsLockedBeforeRead = Statistics.getNumAbortsLockedBeforeRead()
                - numAbortsLockedBeforeRead;
        numAbortsLockedBeforeElasticRead = Statistics
                .getNumAbortsLockedBeforeElasticRead()
                - numAbortsLockedBeforeElasticRead;
        numAbortsLockedOnRead = Statistics.getNumAbortsLockedOnRead()
                - numAbortsLockedOnRead;
        numAbortsInvalidCommit = Statistics.getNumAbortsInvalidCommit()
                - numAbortsInvalidCommit;
        numAbortsInvalidSnapshot = Statistics.getNumAbortsInvalidSnapshot()
                - numAbortsInvalidSnapshot;

        assert (numAborts == (numAbortsBetweenSuccessiveReads
                + numAbortsBetweenReadAndWrite + numAbortsExtendOnRead
                + numAbortsWriteAfterRead + numAbortsLockedOnWrite
                + numAbortsLockedBeforeRead + numAbortsLockedBeforeElasticRead
                + numAbortsLockedOnRead + numAbortsInvalidCommit + numAbortsInvalidSnapshot));

        assert (numStarts - numAborts) == numCommits;

        readSetSizeSum = Statistics.getSumReadSetSize() - readSetSizeSum;
        writeSetSizeSum = Statistics.getSumWriteSetSize() - writeSetSizeSum;
        statSize = Statistics.getStatSize() - statSize;
        txDurationSum = Statistics.getSumCommitingTxTime() - txDurationSum;

        printLine('-');
        System.out.println("TM statistics");
        printLine('-');

        System.out.println("  Commits:                  \t" + numCommits);
        System.out
                .println("  |--regular read only  (%) \t"
                        + numCommitsReadOnly
                        + "\t( "
                        + formatDouble(((double) numCommitsReadOnly / (double) numCommits) * 100)
                        + " %)");
        System.out
                .println("  |--elastic (%)            \t"
                        + numCommitsElastic
                        + "\t( "
                        + formatDouble(((double) numCommitsElastic / (double) numCommits) * 100)
                        + " %)");
        System.out
                .println("  |--regular update (%)     \t"
                        + numCommitsUpdate
                        + "\t( "
                        + formatDouble(((double) numCommitsUpdate / (double) numCommits) * 100)
                        + " %)");
        System.out.println("  Starts:                   \t" + numStarts);
        System.out.println("  Aborts:                   \t" + numAborts
                + "\t( 100 %)");
        System.out
                .println("  |--between succ. reads:   \t"
                        + (numAbortsBetweenSuccessiveReads)
                        + "\t( "
                        + formatDouble(((double) (numAbortsBetweenSuccessiveReads) * 100)
                        / (double) numAborts) + " %)");
        System.out
                .println("  |--between read & write:  \t"
                        + numAbortsBetweenReadAndWrite
                        + "\t( "
                        + formatDouble(((double) numAbortsBetweenReadAndWrite / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--extend upon read:      \t"
                        + numAbortsExtendOnRead
                        + "\t( "
                        + formatDouble(((double) numAbortsExtendOnRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--write after read:      \t"
                        + numAbortsWriteAfterRead
                        + "\t( "
                        + formatDouble(((double) numAbortsWriteAfterRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked on write:       \t"
                        + numAbortsLockedOnWrite
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedOnWrite / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked before read:    \t"
                        + numAbortsLockedBeforeRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedBeforeRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked before eread:   \t"
                        + numAbortsLockedBeforeElasticRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedBeforeElasticRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--locked on read:        \t"
                        + numAbortsLockedOnRead
                        + "\t( "
                        + formatDouble(((double) numAbortsLockedOnRead / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--invalid commit:        \t"
                        + numAbortsInvalidCommit
                        + "\t( "
                        + formatDouble(((double) numAbortsInvalidCommit / (double) numAborts) * 100)
                        + " %)");
        System.out
                .println("  |--invalid snapshot:      \t"
                        + numAbortsInvalidSnapshot
                        + "\t( "
                        + formatDouble(((double) numAbortsInvalidSnapshot / (double) numAborts) * 100)
                        + " %)");
        System.out.println("  Read set size on avg.:    \t"
                + formatDouble(readSetSizeSum / statSize));
        System.out.println("  Write set size on avg.:   \t"
                + formatDouble(writeSetSizeSum / statSize));
        System.out.println("  Tx time-to-commit on avg.:\t"
                + formatDouble((double) txDurationSum / numCommits)
                + " microsec");
        System.out.println("  Number of elastic reads       " + elasticReads);
        System.out
                .println("  Number of reads in RO prefix  " + readsInROPrefix);
    }

    /**
     * Print the iteration statistics on the standard output
     */
    private void printIterationStats() {
        printLine('-');
        System.out.println("Iteration statistics");
        printLine('-');

        int n = Parameters.confIterations;
        System.out.println("  Iterations:                 \t" + n);
        double sum = 0;
        int sizeSum = 0;
        for (int i = 0; i < n; i++) {
            sum += ((throughput[i] / 1024) / 1024);
            sizeSum += totalSize[i];
        }
        System.out.println("  Total throughput (mebiops/s): " + sum);
        double mean = sum / n;
        double meanSize = (double) sizeSum / n;
        System.out.println("  |--Mean:                    \t" + mean);
        System.out.println("  |--Mean Total Size:         \t" + meanSize);
        double temp = 0;
        for (int i = 0; i < n; i++) {
            double diff = ((throughput[i] / 1024) / 1024) - mean;
            temp += diff * diff;
        }
        double var = temp / n;
        System.out.println("  |--Variance:                \t" + var);
        double stdevp = java.lang.Math.sqrt(var);
        System.out.println("  |--Standard deviation pop:  \t" + stdevp);
        double sterr = stdevp / java.lang.Math.sqrt(n);
        System.out.println("  |--Standard error:          \t" + sterr);
        System.out.println("  |--Margin of error (95% CL):\t" + (sterr * 1.96));
    }

    private static String formatDouble(double result) {
        Formatter formatter = new Formatter(Locale.US);
        return formatter.format("%.2f", result).out().toString();
    }
}
