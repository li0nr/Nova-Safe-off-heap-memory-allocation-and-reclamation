package com.yahoo.oak;

import java.io.IOException;
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


public class ListNovaRand {
	
	  
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
	
	@State(Scope.Benchmark)
	public static class BenchmarkState {
		public static  int LIST_SIZE = MYParam.G_LIST_SIZE;
		private List_Nova list ;

		@Setup
		public void setup() {
			list= new List_Nova();
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
		Random rand = new Random();
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
	public void ReadNova(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
		int i = 0;
		while(i < MYParam.G_LIST_SIZE/ThreadState.threads) {
			blackhole.consume(state.list.get(threadState.rand.nextInt(MYParam.G_LIST_SIZE),threadState.i));
			i++;
			}
		}
  
	@Warmup(iterations = MYParam.warmups)
	@Measurement(iterations = MYParam.iterations)
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.MILLISECONDS)
	@Fork(value = 0)
	@Benchmark
	public void WriteNova(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
		int i = 0;
		while(i < MYParam.G_LIST_SIZE/ThreadState.threads) {
			blackhole.consume(state.list.set(threadState.rand.nextInt(MYParam.G_LIST_SIZE), i*2,threadState.i));
			i++;
			}
		}
  

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(ListNovaRand.class.getSimpleName())
				.forks(MYParam.forks)
				.threads(4)
				.build();
		new Runner(opt).run();
		}
}
