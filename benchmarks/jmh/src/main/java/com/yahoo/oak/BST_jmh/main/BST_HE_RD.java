package com.yahoo.oak.BST_jmh.main;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
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

import com.yahoo.oak.BST_HE;
import com.yahoo.oak.BST_Nova;
import com.yahoo.oak.Buff;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.RNG;
import com.yahoo.oak.BST_jmh.BSTParam;
import com.yahoo.oak.BST_jmh.main.BST_bench_Nova.BenchmarkState;
import com.yahoo.oak.BST_jmh.main.BST_bench_Nova.ThreadState;



public class BST_HE_RD {
	
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
 	
	@State(Scope.Benchmark)
	public static class BenchmarkState {

    	public static  int Range = 1024*1024*2;
    	public static  int size  = BSTParam.BST_SIZE;
        private BST_HE<Buff,Buff> BST ;
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);


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
        public void fillTree() {
    		Random rand = new Random(208);

    	    BST = new BST_HE<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
					,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
        	for (int i=0; i <size ; i++) {
        		int keyval = rand.nextInt(Range);
        		Buff k = new Buff();
        		Buff v = new Buff();
        		k.set(keyval);
        		v.set(size - keyval);
        		BST.put(k,v, 0);
        		}
        	System.gc();
        	}
    }

	@State(Scope.Thread)
	public static class ThreadState {
		static int threads = -1;
		Random rand = new Random();
		Buff buff = new Buff();
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
	
	
	
  @Warmup(iterations = BSTParam.warmups)
  @Measurement(iterations = BSTParam.iterations)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 0)
  @Benchmark
  public void search90_delete5_insert5(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
  	int i = 0;
  	while( i < BenchmarkState.size/ThreadState.threads) {
  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
  		switch(BenchmarkState.BenchmarkState_90_5_5.Functions_3()) {
  		case(1):
  	      	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
			break;
  		case(2):
  	      	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
			break;
  		case(3):
  	      	blackhole.consume(state.BST.put(threadState.buff,threadState.buff,threadState.i));
  		}
      	i++;
      	}
  	}
  
  @Warmup(iterations = BSTParam.warmups)
  @Measurement(iterations = BSTParam.iterations)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 0)
  @Benchmark
  public void search50_delete25_insert25(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
  	int i = 0;
  	while( i < BenchmarkState.size/ThreadState.threads) {
  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
  		switch(BenchmarkState.BenchmarkState_50_25_25.Functions_3()) {
  		case(1):
  	      	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
			break;
  		case(2):
  	      	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
			break;
  		case(3):
  	      	blackhole.consume(state.BST.put(threadState.buff,threadState.buff,threadState.i));
  		}
  		i++;
      	}
  	}
  
  @Warmup(iterations = BSTParam.warmups)
  @Measurement(iterations = BSTParam.iterations)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 0)
  @Benchmark
  public void delete50_insert50(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
  	int i = 0;
  	while( i < BenchmarkState.size/ThreadState.threads) {
  		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
  		switch(BenchmarkState.BenchmarkState_50_50.Functions_2()) {
  		case(1):
  	      	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
  			break;
  		case(2):
  	      	blackhole.consume(state.BST.put(threadState.buff,threadState.buff,threadState.i));
  		}
  		i++;
      	}
  	}
    
	
//	 @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_Serial(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
//	    		threadState.buff.set(i);
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//	    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_Rand(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
// 	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	if(i% 10 == 0)blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//	    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_RandnD(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
// 	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_Randn3D(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
// 	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//	    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_Randn4D(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
// 	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//
//	    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void ReadBulk_Randn5D(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
// 	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Group("ReadDelete")
//	    @GroupThreads(1)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void readParallel(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i <BenchmarkState.size/ThreadState.threads) {
//	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
//	    
//	    @Warmup(iterations = BSTParam.warmups)
//	    @Measurement(iterations = BSTParam.iterations)
//	    @BenchmarkMode(Mode.AverageTime)
//	    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//	    @Group("ReadDelete")
//	    @GroupThreads(7)
//	    @Fork(value = 0)
//	    @Benchmark
//	    public void deleteParallel(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//	    	int i = 0;
//	    	while( i < BenchmarkState.size/ThreadState.threads) {
//	    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//	        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//	        	i++;
//	    	}
//		}
	    
	    public static void main(String[] args) throws RunnerException {
	    	Options opt = new OptionsBuilder()
	    			.include(BST_HE_RD.class.getSimpleName())
	                .forks(BSTParam.forks)
	                .threads(1)
	                .build();

	    	new Runner(opt).run();
	    }
	}