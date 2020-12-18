package com.yahoo.oak;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

import org.junit.Test;

public class Flags {
	static final boolean Fences	= false;
	static final boolean TAP 	= false;
	static final boolean UNSet 	= false;
	
	
    public  static void Main(String[] args)throws java.io.IOException {
    
    }
    
    @Test
    public void Test() throws java.io.IOException{
    	//SequntialBenchmark.SeqBenchmark("N", "R", 1000, 4);
    	RandomBenchmark.ConcurrentRandBenchmark("N", "R", 1000, 4);

    }
}