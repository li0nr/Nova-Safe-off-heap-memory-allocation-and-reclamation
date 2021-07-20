package com.yahoo.oak;

import java.util.Map;

public class RNG {

    private double[] distribution;

    public RNG(int size) {
        distribution = new double[size];
    }

    public void addNumber(int value, double _distribution) {
    	distribution[value-1] = _distribution*10;
    }

    public int Functions_3() {
        double rand = Math.random()*1000;
        if(rand < distribution[0])
        	return 1;
        else if(rand < distribution[1])
        	return 2;
        else if(rand < distribution[2])
        	return 3;
        return -1;
    }
    
    public int Functions_2() {
        double rand = Math.random()*100;
        if(rand < distribution[0])
        	return 1;
        else if(rand < distribution[1])
        	return 2;
        return -1;
    }
    
    public static void main(String[] args) {
        RNG drng = new RNG(3);
        drng.addNumber(1, 5);
        drng.addNumber(2, 10);
        drng.addNumber(3, 100);

        int[] hist = new int[3];
        int testCount = 10000;

        HashMap<Integer, Double> test = new HashMap<>();

        for (int i = 0; i < testCount; i++) {
            int random = drng.Functions_3();
            hist[random-1]++;
        }
        
        System.out.println("1 was:"+ hist[0]+"\n");
        System.out.println("2 was:"+ hist[1]+"\n");
        System.out.println("3 was:"+ hist[2]+"\n");

    }

}