package com.yahoo.oak;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;



public class ListWriteBenchmark {
	
	static final int warmups=10;
	static final int iterations=60;
	
	static final int thread1=1;
	static final int thread2=2;
	static final int thread4=4;
	static final int thread8=8;
	static final int thread12=12;
	static final int thread16=16;
	static final int thread20=20;
	static final int thread24=24;
	static final int thread32=32;



	  
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
 
    @State(Scope.Benchmark)
    public static class BenchmarkState {
  	  @Param({"1000000"})
  	  public static int LIST_SIZE;
  	  
        private NovaList nlist ;
        private OffHeapList olist ;

        @Setup(Level.Iteration)
        public void setup() {
        	nlist= new NovaList();
        	for (int i=0; i <LIST_SIZE ; i++) {
        		nlist.add((long)i);
        	}
        	olist= new OffHeapList();
        	for (int i=0; i <LIST_SIZE ; i++) {
        		olist.add((long)i);
        	}
        }

//        @TearDown
//        public void tearDown() {
//            System.out.println("Num objects at tearDown " );
//        }

        @TearDown(Level.Iteration)
        public void closeOak() throws IOException {
        	nlist.close();
        	olist.close();
        }

    }

    @State(Scope.Thread)
    public static class ThreadState {
    	int i=-1;
        @Setup
        public void setup() {
        	i=THREAD_INDEX.getAndAdd(1);
        }
    }

    
    /* --------------------------------------------------*****************------------------------------------------------*/

    @Warmup(iterations = warmups)
    @Measurement(iterations = iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(value = 0)
    @Threads(thread1)
    @Benchmark
    public void writeNova(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
        for(int i=threadState.i*BenchmarkState.LIST_SIZE/1 ; 
        		i<threadState.i*BenchmarkState.LIST_SIZE/1  +BenchmarkState.LIST_SIZE/1
        		&& i<BenchmarkState.LIST_SIZE; i++ ) {
        	state.nlist.set(i, 2);
            blackhole.consume(state.nlist);
    	}
    }
    
    @Warmup(iterations = warmups)
    @Measurement(iterations = iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(value = 0)
    @Threads(thread1)
    @Benchmark
    public void readunmanaged(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
        for(int i=threadState.i*BenchmarkState.LIST_SIZE/1 ;
        		i<threadState.i*BenchmarkState.LIST_SIZE/1  +BenchmarkState.LIST_SIZE/1
        		&& i<BenchmarkState.LIST_SIZE; i++ ) {
        	state.olist.set(i, 2);
            blackhole.consume(state.olist);
    	}
    }
   /* --------------------------------------------------*****************------------------------------------------------*/


    //java -jar -Xmx8g -XX:MaxDirectMemorySize=8g ./benchmarks/target/benchmarks.jar put -p numRows=500000 -prof stack
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ListWriteBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}