package com.yahoo.oak;


import java.util.ArrayList;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.HazardEras.HEslice;


public class BaseLine_bench {
    static int iter = 1;
    
    @State(Scope.Benchmark)
    public static class BenchmarkState {
    	
    	Buff v;
    	long nu;
    	NovaSlice Slice;
    	HEslice HESlice;

    	HazardEras HE;
	    ArrayList<Long> F;
	    long[] F2;
	    
        @Setup
        public void setup() {
    	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    	    final NovaManager novaManager = new NovaManager(allocator);
    	    HE = new HazardEras(allocator);
    	    new Facade_Nova(novaManager);
    	    Slice = new NovaSlice(-1,-1,-1);

    	    allocator.allocate(Slice, 8);
    	    nu = Facade_Nova.AllocateSlice(8, 0);
    	    v = new Buff();
    	    v.set(100);
    	    Facade_Nova.WriteFast(Buff.DEFAULT_SERIALIZER, v ,nu,  0);
    	    Buff.DEFAULT_SERIALIZER.serialize(v, Slice.address + Slice.offset);
    	    F = new ArrayList<>();
    	    }
        }
    
    @Benchmark
    public void ReadSlice(Blackhole blackhole,BenchmarkState state) {
    	int i = 0;
    	while(i<iter) {
    	blackhole.consume(Buff.DEFAULT_C.compareKeys(state.Slice.address+state.Slice.offset, state.v));
    	i++;
    	}
    }
    
    @Benchmark
    public void ReadNova(Blackhole blackhole,BenchmarkState state) {
    	int i = 0;
    	while(i<iter) {
    	blackhole.consume(Facade_Nova.Compare(state.v, Buff.DEFAULT_C, state.nu));
    			i++;
    	}
    }
    
    @Benchmark
    public void WriteNova(Blackhole blackhole,BenchmarkState state) {
    	int i = 0;
    	while(i<iter) {
    	blackhole.consume(Facade_Nova.WriteFull(Buff.DEFAULT_SERIALIZER, state.v, state.nu, 0));
    	i++;
    	}
    }
    
    @Benchmark
    public void WriteSlice(Blackhole blackhole,BenchmarkState state) {
    	int i = 0;
    	while(i<iter) {
    		Buff.DEFAULT_SERIALIZER.serialize(state.v, state.Slice.address+state.Slice.offset);
    		i++;
    	}
    }
    
    @Benchmark
    public void HEprt(Blackhole blackhole,BenchmarkState state) {
    	int i = 0;
    	while(i<iter) {
        	blackhole.consume(state.HE.get_protected(state.HESlice, 0));//will not wokr
        	i++;
    	}
    }
    

  
  @Benchmark
  public void Empty(Blackhole blackhole,BenchmarkState state) {
  }
    
    public static void main(String[] args) throws RunnerException {
    	Options opt = new OptionsBuilder()
    			.include(BaseLine_bench.class.getSimpleName())
                .forks(MYParam.forks)
                .threads(1)
                .build();

    	new Runner(opt).run();
    }
    
}
    
