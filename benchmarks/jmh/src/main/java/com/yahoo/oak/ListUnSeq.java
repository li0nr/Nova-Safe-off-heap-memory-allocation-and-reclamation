package com.yahoo.oak;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class ListUnSeq {

	  
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
 
    @State(Scope.Benchmark)
    public static class BenchmarkState {

    	public static  int LIST_SIZE = MYParam.G_LIST_SIZE;
        private ListOffHeap list ;

        @Setup
        public void setup() {
        	list= new ListOffHeap();
        	for (int i=0; i <LIST_SIZE ; i++) {
        		list.add((long)i,0);
        	}
        }

        @TearDown
        public void close() throws IOException {
        	list.close();
        }

    }

    @State(Scope.Thread)
    public static class ThreadState {
    	static int threads = -1;
    	int i=-1;
        @Setup
        public void setup() {
        	i=THREAD_INDEX.getAndAdd(1);
        	if(threads <= i)
        		threads = i +1;
        }
        @TearDown
        public void tear() {
        	THREAD_INDEX.set(0);
        	System.out.println("\n Threads Num: "+ threads);
        }
        
    }
    
    @Warmup(iterations = MYParam.warmups)
    @Measurement(iterations = MYParam.iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 0)
    @Benchmark
    public void ReadUn(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
        for(int i = threadState.i*BenchmarkState.LIST_SIZE/ThreadState.threads; 
        		i < threadState.i*BenchmarkState.LIST_SIZE/ThreadState.threads +
        						  BenchmarkState.LIST_SIZE/ThreadState.threads
        		&& i<BenchmarkState.LIST_SIZE; i++ ) {
        	blackhole.consume(state.list.get( i,threadState.i));
    	}
    }
    
    
    @Warmup(iterations = MYParam.warmups)
    @Measurement(iterations = MYParam.iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 0)
    @Benchmark
    public void WriteUn(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
        for(int i = threadState.i*BenchmarkState.LIST_SIZE/ThreadState.threads; 
        		i < threadState.i*BenchmarkState.LIST_SIZE/ThreadState.threads +
        						  BenchmarkState.LIST_SIZE/ThreadState.threads
        		&& i<BenchmarkState.LIST_SIZE; i++ ) {
        	state.list.set( i,2 *i,threadState.i);
    	}
    }
    
    
    
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ListUnSeq.class.getSimpleName())
                .forks(MYParam.forks)
                .threads(4)
                .build();

        new Runner(opt).run();
    }

}