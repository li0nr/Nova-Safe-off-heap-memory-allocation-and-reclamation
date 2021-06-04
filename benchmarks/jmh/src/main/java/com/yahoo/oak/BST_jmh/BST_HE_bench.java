package com.yahoo.oak.BST_jmh;

import java.util.Random;
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

import com.yahoo.oak.BST_HE;
import com.yahoo.oak.Buff;
import com.yahoo.oak.MYParam;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.BST_jmh.BST_NoMM_bench.ThreadState;
import com.yahoo.oak.BST_jmh.BST_Nova_bench.BenchmarkState;

public class BST_HE_bench {
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
	 
    @State(Scope.Benchmark)
    public static class BenchmarkState {

    	public static  int size  = BSTParam.BST_SIZE;
        private BST_HE<Buff,Buff> BST ;

        @Setup
        public void setup() {
    	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	    
    	    BST = new BST_HE<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
										,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
        	for (int i=0; i <size ; i++) {
        		Buff k = new Buff();
        		Buff v = new Buff();
        		k.set(i);
        		v.set(size-i);
        		BST.put(k,v, 0);
        	}
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
			buff.set(BenchmarkState.size);
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
    public void Read(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
    	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
	}
    
    @Warmup(iterations = MYParam.warmups)
    @Measurement(iterations = MYParam.iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 0)
    @Benchmark
    public void ReadBulk_Serial(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
    	int i = 0;
    	while( i < BenchmarkState.size/ThreadState.threads) {
    		threadState.buff.set(i);
        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
        	i++;
    	}
	}
    
    @Warmup(iterations = MYParam.warmups)
    @Measurement(iterations = MYParam.iterations)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 0)
    @Benchmark
    public void ReadBulk_Rand(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
    	int i = 0;
    	while( i < BenchmarkState.size/ThreadState.threads) {
    		threadState.buff.set(threadState.rand.nextInt(BenchmarkState.size));
        	blackhole.consume(state.BST.containsKey(threadState.buff,threadState.i));
        	i++;
    	}
	}
    
    
    
    public static void main(String[] args) throws RunnerException {
    	Options opt = new OptionsBuilder()
    			.include(BST_HE_bench.class.getSimpleName())
                .forks(MYParam.forks)
                .threads(4)
                .build();

    	new Runner(opt).run();
    }
}