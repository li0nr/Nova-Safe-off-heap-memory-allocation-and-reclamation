package com.yahoo.oak;

import java.util.List;

public class benchMath {
	
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

}
