package com.nova.bencmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

public class bench_Math {
	
    static double Mean(List<Long> means) {
    	int n=0;
    	double sum=0, mean=0;
    	for (double s : means) {
    		sum=sum+s;
			n++;
			}
		return sum/n;
    }
    
    static double StandardDeviation(List<Long> means) {
    	int n=0;
    	double sum=0, mean=0;
    	for (double s : means) {
    		sum=sum+s;
			n++;
			}
		mean=sum/n;
		sum=0;  
    	for (double s : means) {
			sum+=Math.pow((s-mean),2);
    	}
		mean=sum/(n);
		return Math.sqrt(mean);
		
    }
    
    static double StandardError(List<Long> means) {
    	int n=0;
    	double sum=0, mean=0;
    	for (double s : means) {
    		sum=sum+s;
			n++;
			}
		mean=sum/n;
		sum=0;  
    	for (double s : means) {
			sum+=Math.pow((s-mean),2);
    	}
		mean=sum/(n-1);
		mean=Math.sqrt(mean);
		return mean/Math.sqrt(n);
		
    }
    
    @Test
    public void test() { 	
        int i=0;
        ThreadLocalRandom random = ThreadLocalRandom.current();  
        while(i<100) {
        	System.out.println("round"+i+"value"+random.current().nextInt(1000_000)+"\n");
        	i++;
        }
        
        ArrayList<Long> a= new ArrayList<Long>();
    	for ( i=0; i<10 ; i++) {
    		long n = (i);
    		a.add(n);
    	}
    	double dt=bench_Math.StandardDeviation(a);
    	double se=bench_Math.StandardError(a);
    	double mean= bench_Math.Mean(a);
    	System.out.println("Mean:"+mean);
    	System.out.println("sd :"+dt);
    	System.out.println("se :"+se);
    }

}
