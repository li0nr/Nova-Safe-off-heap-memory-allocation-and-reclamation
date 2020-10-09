package com.yahoo.oak;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class Main {
    public  static void main(String[] args) {
    	NovaListBenchmark a= new NovaListBenchmark();
        try {
            FileWriter myWriter = new FileWriter("filename.txt");
            String modes[] = {"R"};
            int threads[] = {1};
            for (String s: modes)
            	for (int thread : threads)
            		a.RunBenchmark(thread, 1000000,myWriter ,s);
            myWriter.close();
        }

        catch (Exception e) {}
    }
}
