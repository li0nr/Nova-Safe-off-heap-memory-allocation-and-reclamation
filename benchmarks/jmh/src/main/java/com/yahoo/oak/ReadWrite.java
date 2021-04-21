package com.yahoo.oak;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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

import com.yahoo.oak.ListHERand.ThreadState;

public class ReadWrite {

		
	final static  AtomicInteger THREAD_INDEX = new AtomicInteger(0);
	
	@State(Scope.Benchmark)
	public static class BenchmarkState {	
		public static  int LIST_SIZE = MYParam.G_LIST_SIZE;
		public static  int LIST_SIZE_2 = LIST_SIZE/2;
		
		private List_OffHeap listo ;
		private List_Nova    listn ;
		private List_HE      listh ;

		    @Setup
		    public void setup() {
		    	listo= new List_OffHeap();
		    	listn= new List_Nova();
		    	listh= new List_HE();

		    	for (int i=0; i <LIST_SIZE ; i++) {
		    		listo.add((long)i,0);		    		
		    		listn.add((long)i,0);
		    		listh.add((long)i,0);

		    	}
		    }
		
		    @TearDown
		    public void close() throws IOException {
		    	listo.close();
		    	listn.close();		    	
		    	listh.close();
		    }
		
		}

	
	
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("HE")
	    @GroupThreads(15)
		@Fork(value = 0)
		@Benchmark
		public void HERead(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	blackhole.consume(
		    			state.listh.get(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),
		    							threadState.i)
		    			);
		    	i++;
			}
		}
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("HE")
	    @GroupThreads(1)
		@Fork(value = 0)
		@Benchmark
		public void HEWrite(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	state.listh.set(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),i,threadState.i);
		    	i++;
			}
		}
		
		
		/*-----------------------------------------------------------------------------*/
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("Nova")
	    @GroupThreads(15)
		@Fork(value = 0)
		@Benchmark
		public void NovaRead(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	blackhole.consume(
		    			state.listh.get(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),
		    							threadState.i)
		    			);
		    	i++;
			}
		}
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("Nova")
	    @GroupThreads(1)
		@Fork(value = 0)
		@Benchmark
		public void NovaWrite(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	state.listh.set(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),i,threadState.i);
		    	i++;
			}
		}
		
		
		/*-----------------------------------------------------------------------------*/
		
		
		/*-----------------------------------------------------------------------------*/
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("Un")
	    @GroupThreads(15)
		@Fork(value = 0)
		@Benchmark
		public void UnRead(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	blackhole.consume(
		    			state.listh.get(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),
		    							threadState.i)
		    			);
		    	i++;
			}
		}
		
		@Warmup(iterations = MYParam.warmups)
		@Measurement(iterations = MYParam.iterations)
		@BenchmarkMode(Mode.AverageTime)
		@OutputTimeUnit(TimeUnit.MILLISECONDS)
		@Group("Un")
	    @GroupThreads(1)
		@Fork(value = 0)
		@Benchmark
		public void UnWrite(Blackhole blackhole,BenchmarkState state,ThreadState threadState) {
			  int i = 0;
			  while(i < MYParam.Limit) {
		    	state.listh.set(BenchmarkState.LIST_SIZE_2+ threadState.rand.nextInt(MYParam.range),i,threadState.i);
		    	i++;
			}
		}
		
		
		/*-----------------------------------------------------------------------------*/
		  public static void main(String[] args) throws RunnerException {
		      Options opt = new OptionsBuilder()
		              .include(ReadWrite.class.getSimpleName())
		              .forks(MYParam.forks)
		              .threads(4)
		              .build();

		      new Runner(opt).run();
		  }
		 
		
}
