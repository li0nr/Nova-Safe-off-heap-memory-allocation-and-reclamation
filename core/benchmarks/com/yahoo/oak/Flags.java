package com.yahoo.oak;
import org.junit.Test;

public class Flags {
	static final boolean Fences	= true;
	static final boolean TAP 	= true;
	static final boolean UNSet 	= true;
	
    @Test
    public void Test() throws java.io.IOException{
    	//SequntialBenchmark.SeqBenchmark("N", "R", 1000, 4);
    	RandomBenchmark.ConcurrentRandBenchmark("N", "R", 1000, 4);

    }
}