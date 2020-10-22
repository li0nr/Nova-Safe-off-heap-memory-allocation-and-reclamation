package com.yahoo.oak;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class Main {
    public  static void main(String[] args)throws java.io.IOException {
    	NovaListBenchmark a= new NovaListBenchmark();
    	  if(args.length == 0) {
    		  String modes[]= {"R","W","RW"};
              int threads[]= {4};
              for (String s: modes)
              	for (int thread : threads)
              		a.RunBenchmark(thread, 1_000_000,null ,s);
    	  }else {
    		  String mode = args[0];
    		  int num = Integer.parseInt(args[1]);
    		  a.RunBenchmark(num,1000 , null, mode);

    	  }
    }
}
