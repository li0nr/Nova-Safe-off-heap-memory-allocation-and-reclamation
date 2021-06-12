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

import com.yahoo.oak.BST_NoMM;
import com.yahoo.oak.Buff;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.ParamBench;
import com.yahoo.oak.RNG;
import com.yahoo.oak.BST_jmh.BSTParam;

public class BST_bench_NoMM {
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);

	@State(Scope.Benchmark)
	public static class BenchmarkState {

    	public static  int size  = BSTParam.BST_SIZE;
        private BST_NoMM<Buff,Buff> BST ;
	    public static NativeMemoryAllocator allocator;


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
    		if(allocator != null)
    			ParamBench.PrintMem(allocator);
    		
    		allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	    BST = new BST_NoMM<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
					,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
        	for (int i=0; i <0.75 *size ; i++) {
        		int keyval = rand.nextInt(size);
        		Buff k = new Buff();
        		Buff v = new Buff();
        		k.set(keyval);
        		v.set(size - keyval);
        		if(BST.put(k,v, 0) != null)
        			i--;
        		}
        	}
        
        @TearDown(Level.Iteration)
        public void printStats() {
        	ParamBench.PrintMem(allocator);
        	
			System.out.println("\n gets Num iter : "+ BST.get_count);
			System.out.println("\n dels Num iter : "+ BST.del_count);
			System.out.println("\n puts Num iter : "+ BST.put_count);

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
    
    
//    @Warmup(iterations = BSTParam.warmups)
//    @Measurement(iterations = BSTParam.iterations)
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @Fork(value = 0)
//    @Benchmark
//    public void ReadBulk_Serial(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//    	int i = 0;
//    	while( i < BenchmarkState.size/ThreadState.threads) {
//    		threadState.buff.set(i);
//        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//        	i++;
//    	}
//	}
//    
//    @Warmup(iterations = BSTParam.warmups)
//    @Measurement(iterations = BSTParam.iterations)
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @Fork(value = 0)
//    @Benchmark
//    public void ReadBulk_Rand(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//    	int i = 0;
//    	while( i < BenchmarkState.size/ThreadState.threads) {
//    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//        	if(i%10 == 0) blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//        	i++;
//    	}
//	}
//     
//    
//    @Warmup(iterations = BSTParam.warmups)
//    @Measurement(iterations = BSTParam.iterations)
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @Group("ReadDelete")
//    @GroupThreads(1)
//    @Fork(value = 0)
//    @Benchmark
//    public void readParallel(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//    	int i = 0;
//    	while( i <BenchmarkState.size/ThreadState.threads) {
//    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
//        	i++;
//    	}
//	}
//    
//    @Warmup(iterations = BSTParam.warmups)
//    @Measurement(iterations = BSTParam.iterations)
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @Group("ReadDelete")
//    @GroupThreads(7)
//    @Fork(value = 0)
//    @Benchmark
//    public void deleteParallel(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
//    	int i = 0;
//    	while( i < BenchmarkState.size/ThreadState.threads) {
//    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
//        	blackhole.consume(state.BST.remove(threadState.buff,threadState.i));
//        	i++;
//    	}
//	}
    
    
    public static void main(String[] args) throws RunnerException {
    	Options opt = new OptionsBuilder()
    			.include(BST_bench_NoMM.class.getSimpleName())
                .forks(BSTParam.forks)
                .threads(1)
                .build();

    	new Runner(opt).run();
    }
}
