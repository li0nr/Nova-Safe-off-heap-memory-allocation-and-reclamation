package com.yahoo.oak.SA_jmh;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.ParamBench;
import com.yahoo.oak.RNG;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_NoMM;



public class SA_bench_NoMM {
	
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
 	
	@State(Scope.Benchmark)
	public static class BenchmarkState {

    	public static  int size  = SAParam.LL_Size;
    	public static  NativeMemoryAllocator allocator;
        private SA_NoMM SA;

    	static RNG BenchmarkState_90_5_5 = 	 new RNG(3);
    	static RNG BenchmarkState_50_25_25 = new RNG(3);
    	static RNG BenchmarkState_50_50 = 	 new RNG(2);

    	@Setup
    	public void setRandom() {
    		BenchmarkState_90_5_5.addNumber(1, 90);
    		BenchmarkState_90_5_5.addNumber(2, 95);
    		BenchmarkState_90_5_5.addNumber(3, 100);
    		
    		/*****************************************/
    		
    		BenchmarkState_50_25_25.addNumber(1, 50);
    		BenchmarkState_50_25_25.addNumber(2, 75);
    		BenchmarkState_50_25_25.addNumber(3, 100);
    		
    		
    		/*****************************************/
    		
    		BenchmarkState_50_50.addNumber(1, 50);
    		BenchmarkState_50_50.addNumber(2, 100);


    	}
	    
        @Setup(Level.Iteration)
        public void fill() {
    	    SA = new SA_NoMM(SAParam.LL_Size, Buff.DEFAULT_SERIALIZER);
    	    SA.ParallelFill(SAParam.LL_Size);
        }
        @TearDown(Level.Iteration)
        public void printStats() {
        	
			ParamBench.PrintMem(SA.getAlloc());
			SA.getAlloc().FreeNative();
			SA = null;
        }
    }

	@State(Scope.Thread)
	public static class ThreadState {
		static int threads = -1;
		Random rand = new Random();
		Buff buff = new Buff(1024);
		int i=-1;
		
		@Setup
		public void setup() {
			buff.set(0);
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
	
	
	
	  @Warmup(iterations = SAParam.warmups)
	  @Measurement(iterations = SAParam.iterations)
	  @BenchmarkMode(Mode.AverageTime)
	  @OutputTimeUnit(TimeUnit.MILLISECONDS)
	  @Fork(value = 0)
	  @OperationsPerInvocation(SAParam.LL_Size)
	  @Benchmark
	  public void search90_delete5_insert5(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
	  	int i = 0;
	  	while( i < BenchmarkState.size/ThreadState.threads) {
	  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
	  		switch(BenchmarkState.BenchmarkState_90_5_5.Functions_3()) {
	  		case(1):
	  	      	blackhole.consume(state.SA.get(threadState.rand.nextInt(BenchmarkState.size), 
	  	      			Buff.DEFAULT_R,threadState.i));
				break;
	  		case(2):
	  	      	blackhole.consume(state.SA.delete(threadState.rand.nextInt(BenchmarkState.size)
	  	      			,threadState.i));
				break;
	  		case(3):
	  	      	blackhole.consume(state.SA.set(threadState.rand.nextInt(BenchmarkState.size)
	  	      			, threadState.buff,threadState.i));
	  		}
	      	i++;
	      	}
	  	}
	  
	  @Warmup(iterations = SAParam.warmups)
	  @Measurement(iterations = SAParam.iterations)
	  @BenchmarkMode(Mode.AverageTime)
	  @OutputTimeUnit(TimeUnit.MILLISECONDS)
	  @Fork(value = 0)
	  @OperationsPerInvocation(SAParam.LL_Size)
	  @Benchmark
	  public void search50_delete25_insert25(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
	  	int i = 0;
	  	while( i < BenchmarkState.size/ThreadState.threads) {
	  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
	  		switch(BenchmarkState.BenchmarkState_50_25_25.Functions_3()) {
	  		case(1):
	  	      	blackhole.consume(state.SA.get(threadState.rand.nextInt(BenchmarkState.size), 
	  	      			Buff.DEFAULT_R,threadState.i));
				break;
	  		case(2):
	  	      	blackhole.consume(state.SA.delete(threadState.rand.nextInt(BenchmarkState.size)
	  	      			,threadState.i));
				break;
	  		case(3):
	  	      	blackhole.consume(state.SA.set(threadState.rand.nextInt(BenchmarkState.size)
	  	      			, threadState.buff,threadState.i));
	  		}
	  		i++;
	  		}
	  	}
	  
	  @Warmup(iterations = SAParam.warmups)
	  @Measurement(iterations = SAParam.iterations)
	  @BenchmarkMode(Mode.AverageTime)
	  @OutputTimeUnit(TimeUnit.MILLISECONDS)
	  @Fork(value = 0)
	  @OperationsPerInvocation(SAParam.LL_Size)
	  @Benchmark
	  public void delete50_insert50(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
	  	int i = 0;
	  	while( i < BenchmarkState.size/ThreadState.threads) {
	  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
	  		switch(BenchmarkState.BenchmarkState_50_50.Functions_2()) {
	  		case(1):
	  			blackhole.consume(state.SA.delete(threadState.rand.nextInt(BenchmarkState.size)
	  	      			,threadState.i));
	  		break;
	  		case(2):
	  	      	blackhole.consume(state.SA.set(threadState.rand.nextInt(BenchmarkState.size)
	  	      			, threadState.buff,threadState.i));
	  		}
	  		i++;
	  		}
	  	}
  

	    public static void main(String[] args) throws RunnerException {
	    	Options opt = new OptionsBuilder()
	    			.include(SA_bench_NoMM.class.getSimpleName())
	                .forks(SAParam.forks)
	                .threads(1)
	                .build();

	    	new Runner(opt).run();
	    }
	}